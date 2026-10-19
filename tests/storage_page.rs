//! Phase 1 integration tests: on-disk binary format, page/record packing,
//! CRC integrity and the scanner's recovery behaviour — everything below the
//! engine facade, driven directly through `PageWriter` and the scanner.

use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};

use byteorder::{ByteOrder, LittleEndian as LE};

use chronodb::error::ChronoError;
use chronodb::storage::file::{AlignedBuf, CreateOptions, DbFile};
use chronodb::storage::page::{
    page_offset, stamp_page_crc, PAGE_HEADER_SIZE, PAGE_SIZE, PAGE_SIZE_U64,
};
use chronodb::storage::record::{
    RecordHeader, RECORD_HEADER_SIZE, RH_OFF_LSN, RH_OFF_PAYLOAD_SIZE,
};
use chronodb::storage::scanner::scan_and_validate;
use chronodb::storage::writer::{PageWriter, StagedRecord, WriterOptions};

fn fresh_db(dir: &tempfile::TempDir, pages: u64) -> (PathBuf, DbFile) {
    let path = dir.path().join("test.cdb");
    let (db, _sb) = DbFile::create(
        &path,
        &CreateOptions {
            preallocate_pages: pages,
            strict_direct_io: false,
        },
    )
    .expect("create db");
    (path, db)
}

fn writer_for(db: DbFile) -> PageWriter {
    PageWriter::create_for(db, WriterOptions::default()).expect("page writer")
}

/// Reads page 1 into a page-aligned buffer.
fn read_page1(path: &Path) -> AlignedBuf {
    let (db, _sb) = DbFile::open(path).unwrap();
    let mut buf = AlignedBuf::zeroed(PAGE_SIZE, PAGE_SIZE).unwrap();
    db.read_page(1, &mut buf).unwrap();
    buf
}

fn write_page1(path: &Path, buf: &AlignedBuf) {
    let (db, _sb) = DbFile::open(path).unwrap();
    db.write_pages(1, buf, 1).unwrap();
    db.sync_data().unwrap();
}

fn flip_byte(path: &Path, offset: u64) {
    let f = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(path)
        .unwrap();
    let mut b = [0u8; 1];
    f.read_exact_at(&mut b, offset).unwrap();
    b[0] ^= 0xFF;
    f.write_all_at(&b, offset).unwrap();
    f.sync_all().unwrap();
}

/// Collects `(lsn, entity, payload)` for every record in a healthy file.
fn collect(
    path: &Path,
) -> (
    chronodb::storage::scanner::ScanReport,
    Vec<(u64, u128, Vec<u8>)>,
) {
    let (db, _sb) = DbFile::open(path).unwrap();
    let mut seen = Vec::new();
    let report = scan_and_validate(&db, false, |rec| {
        seen.push((rec.lsn(), rec.entity_id(), rec.payload.to_vec()));
        Ok(())
    })
    .unwrap();
    (report, seen)
}

#[test]
fn superblock_roundtrips_through_disk() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("sb.cdb");
    let (db, sb) = DbFile::create(
        &path,
        &CreateOptions {
            preallocate_pages: 8,
            strict_direct_io: false,
        },
    )
    .unwrap();
    assert_eq!(db.len().unwrap(), 8 * PAGE_SIZE_U64);
    drop(db);

    let (db2, sb2) = DbFile::open(&path).unwrap();
    assert_eq!(sb2.cluster_id(), sb.cluster_id());
    assert_eq!(sb2.version(), 1);
    assert_eq!(sb2.page_size(), PAGE_SIZE as u32);
    assert_eq!(sb2.created_at_ns(), sb.created_at_ns());
    assert!(!sb2.strict_direct_io());
    assert_eq!(db2.len().unwrap(), 8 * PAGE_SIZE_U64);
}

#[test]
fn records_roundtrip_with_page_invariants() {
    let dir = tempfile::tempdir().unwrap();
    let (path, db) = fresh_db(&dir, 0);
    let mut w = writer_for(db);

    let a = vec![0x11u8; 100];
    let b = vec![0x22u8; 2500];
    let c = b"hello chronodb".to_vec();
    assert_eq!(w.append(StagedRecord::new(0xAA, 10, &a)).unwrap(), 1);
    assert_eq!(w.append(StagedRecord::new(0xBB, 20, &b)).unwrap(), 2);
    assert_eq!(w.append(StagedRecord::new(0xCC, 30, &c)).unwrap(), 3);
    assert_eq!(w.sync().unwrap(), 3);
    drop(w);

    let (report, seen) = collect(&path);
    assert_eq!(report.record_count, 3);
    assert_eq!(report.min_lsn, Some(1));
    assert_eq!(report.max_lsn, Some(3));
    assert_eq!(report.pages.len(), 1, "2746 used bytes fit one page");
    let p0 = &report.pages[0];
    assert_eq!(p0.page_epoch, 1);
    assert_eq!(p0.prev_page_lsn, 0);
    assert!(p0.is_closed());
    assert_eq!(p0.record_count, 3);
    assert_eq!(seen, vec![(1, 0xAA, a), (2, 0xBB, b), (3, 0xCC, c)]);
}

#[test]
fn staging_flush_writes_many_pages_with_lsn_chain() {
    let dir = tempfile::tempdir().unwrap();
    let (path, db) = fresh_db(&dir, 0);
    let mut w = writer_for(db);

    let payload = vec![0x5Au8; 100];
    for i in 0..200u64 {
        assert_eq!(
            w.append(StagedRecord::new(i as u128, 1_000 + i, &payload))
                .unwrap(),
            i + 1
        );
    }
    assert_eq!(w.sync().unwrap(), 200);
    drop(w);

    let (report, seen) = collect(&path);
    assert_eq!(report.record_count, 200);
    assert_eq!(seen.len(), 200);
    // 144-byte records: 28 per page, so 200 records = 8 pages.
    assert_eq!(report.pages.len(), 8);
    for (i, p) in report.pages.iter().enumerate() {
        assert_eq!(p.page_epoch, i as u64 + 1, "page_epoch tracks the index");
        assert!(p.is_closed());
    }
    // prev_page_lsn(P) = highest LSN touching page P-1.
    assert_eq!(report.pages[0].prev_page_lsn, 0);
    for w in report.pages.windows(2) {
        assert_eq!(w[1].prev_page_lsn, w[0].max_lsn);
    }
    for (i, (lsn, _, payload)) in seen.iter().enumerate() {
        assert_eq!(*lsn, i as u64 + 1);
        assert_eq!(payload.len(), 100);
    }
}

#[test]
fn spanning_record_roundtrips() {
    let dir = tempfile::tempdir().unwrap();
    let (path, db) = fresh_db(&dir, 0);
    let mut w = writer_for(db);

    let big: Vec<u8> = (0..9000u32).map(|i| (i % 251) as u8).collect();
    let tail = b"after the span".to_vec();
    assert_eq!(w.append(StagedRecord::new(0xB160, 1, &big)).unwrap(), 1);
    assert_eq!(w.append(StagedRecord::new(0x2, 2, &tail)).unwrap(), 2);
    assert_eq!(w.sync().unwrap(), 2);
    drop(w);

    let (db2, _sb) = DbFile::open(&path).unwrap();
    let mut seen = Vec::new();
    let report = scan_and_validate(&db2, false, |rec| {
        seen.push((rec.lsn(), rec.payload.to_vec(), rec.pages_spanned));
        Ok(())
    })
    .unwrap();

    // 4020 + 4064 + 916 payload bytes ⇒ three pages.
    assert_eq!(report.record_count, 2);
    assert_eq!(report.pages.len(), 3);
    assert_eq!(seen[0].0, 1);
    assert_eq!(seen[0].1, big);
    assert_eq!(seen[0].2, 3, "record spans exactly three pages");
    assert_eq!(seen[1], (2, tail, 1));
    assert!(
        report.pages[1].starts_with_continuation(),
        "the second page starts mid-record"
    );
    assert!(report.pages[2].starts_with_continuation());
    assert!(!report.pages[0].starts_with_continuation());
}

#[test]
fn tombstone_and_empty_payload_roundtrip() {
    let dir = tempfile::tempdir().unwrap();
    let (path, db) = fresh_db(&dir, 0);
    let mut w = writer_for(db);

    assert_eq!(w.append(StagedRecord::tombstone(0xDD, 5)).unwrap(), 1);
    assert_eq!(w.append(StagedRecord::new(0xEE, 6, &[])).unwrap(), 2);
    assert_eq!(w.append(StagedRecord::new(0xFF, 7, b"x")).unwrap(), 3);
    assert_eq!(w.sync().unwrap(), 3);
    drop(w);

    let (db2, _sb) = DbFile::open(&path).unwrap();
    let mut rows = Vec::new();
    scan_and_validate(&db2, false, |rec| {
        rows.push((rec.lsn(), rec.is_tombstone(), rec.payload.to_vec()));
        Ok(())
    })
    .unwrap();
    assert_eq!(
        rows,
        vec![
            (1, true, Vec::new()),
            (2, false, Vec::new()),
            (3, false, b"x".to_vec()),
        ]
    );
}

#[test]
fn oversized_record_is_rejected_before_consuming_an_lsn() {
    let dir = tempfile::tempdir().unwrap();
    let (path, db) = fresh_db(&dir, 0);
    let mut w = writer_for(db);

    let huge = vec![0u8; (1 << 20) + 1];
    let err = w.append(StagedRecord::new(1, 1, &huge)).unwrap_err();
    assert!(matches!(
        err,
        ChronoError::RecordTooLarge { size, max } if size == (1 << 20) + 1 && max == 1 << 20
    ));
    // The rejection must not burn an LSN.
    assert_eq!(w.append(StagedRecord::new(1, 2, b"ok")).unwrap(), 1);
    assert_eq!(w.sync().unwrap(), 1);
    drop(w);

    let (report, seen) = collect(&path);
    assert_eq!(report.record_count, 1);
    assert_eq!(seen[0].0, 1);
    assert_eq!(seen[0].2, b"ok");
}

#[test]
fn empty_database_scans_to_zero_records_with_free_tail() {
    let dir = tempfile::tempdir().unwrap();
    let (path, db) = fresh_db(&dir, 16);

    let mut n = 0;
    let report = scan_and_validate(&db, false, |_rec| {
        n += 1;
        Ok(())
    })
    .unwrap();
    assert_eq!(n, 0);
    assert_eq!(report.record_count, 0);
    assert!(report.pages.is_empty());
    assert_eq!(report.free_tail_pages, 15, "pages 1..=15 are preallocation");
    drop(db);

    // Nothing on disk changed beyond the superblock.
    assert_eq!(std::fs::metadata(&path).unwrap().len(), 16 * PAGE_SIZE_U64);
}

#[test]
fn page_crc_bit_flip_is_detected_and_repairable() {
    let dir = tempfile::tempdir().unwrap();
    let (path, db) = fresh_db(&dir, 0);
    let mut w = writer_for(db);
    // Three 2000-byte payloads cannot share a page: 2044 + 2044 > 4064.
    for i in 0..3u64 {
        w.append(StagedRecord::new(i as u128, i, &vec![0x77u8; 2000]))
            .unwrap();
    }
    w.sync().unwrap();
    drop(w);
    assert_eq!(std::fs::metadata(&path).unwrap().len(), 4 * PAGE_SIZE_U64);

    // Damage a payload byte in the middle of page 1.
    flip_byte(&path, page_offset(1) + 500);

    let (db2, _sb) = DbFile::open(&path).unwrap();
    let err = scan_and_validate(&db2, false, |_rec| Ok(())).unwrap_err();
    assert!(
        matches!(
            err,
            ChronoError::PageChecksumMismatch { page_index: 1, .. }
                | ChronoError::CorruptPage { page_index: 1, .. }
        ),
        "unexpected error: {err:?}"
    );
    drop(db2);

    // Repair: truncate back to the last valid page boundary.
    let (db3, _sb) = DbFile::open(&path).unwrap();
    let mut n = 0;
    let report = scan_and_validate(&db3, true, |_rec| {
        n += 1;
        Ok(())
    })
    .unwrap();
    assert_eq!(n, 0);
    assert_eq!(report.truncated_bytes, Some(3 * PAGE_SIZE_U64));
    drop(db3);
    assert_eq!(std::fs::metadata(&path).unwrap().len(), PAGE_SIZE_U64);
}

#[test]
fn record_crc_bit_flip_is_detected_when_page_crc_is_valid() {
    let dir = tempfile::tempdir().unwrap();
    let (path, db) = fresh_db(&dir, 0);
    let mut w = writer_for(db);
    w.append(StagedRecord::new(0x99, 1, &[0x42u8; 64])).unwrap();
    w.sync().unwrap();
    drop(w);

    // Flip a payload byte and re-stamp the page CRC so only the record
    // checksum can catch it.
    let mut buf = read_page1(&path);
    {
        let bytes = buf.as_mut_slice();
        let payload_at = PAGE_HEADER_SIZE + RECORD_HEADER_SIZE + 3;
        bytes[payload_at] ^= 0xFF;
    }
    {
        let page: &mut [u8; PAGE_SIZE] = buf.as_mut_slice().try_into().unwrap();
        stamp_page_crc(page);
    }
    write_page1(&path, &buf);

    let (db2, _sb) = DbFile::open(&path).unwrap();
    let err = scan_and_validate(&db2, false, |_rec| Ok(())).unwrap_err();
    assert!(
        matches!(
            err,
            ChronoError::RecordChecksumMismatch { lsn: 1 } | ChronoError::CorruptRecord { .. }
        ),
        "unexpected error: {err:?}"
    );
}

#[test]
fn scanner_detects_wrong_page_epoch() {
    let dir = tempfile::tempdir().unwrap();
    let (path, db) = fresh_db(&dir, 0);
    let mut w = writer_for(db);
    w.append(StagedRecord::new(1, 1, b"epoch")).unwrap();
    w.sync().unwrap();
    drop(w);

    let mut buf = read_page1(&path);
    {
        let bytes = buf.as_mut_slice();
        LE::write_u64(&mut bytes[0..8], 42); // page_epoch must equal the index
    }
    {
        let page: &mut [u8; PAGE_SIZE] = buf.as_mut_slice().try_into().unwrap();
        stamp_page_crc(page);
    }
    write_page1(&path, &buf);

    let (db2, _sb) = DbFile::open(&path).unwrap();
    let err = scan_and_validate(&db2, false, |_rec| Ok(())).unwrap_err();
    assert!(
        matches!(err, ChronoError::CorruptPage { page_index: 1, .. }),
        "unexpected error: {err:?}"
    );
}

#[test]
fn scanner_detects_lsn_gap() {
    let dir = tempfile::tempdir().unwrap();
    let (path, db) = fresh_db(&dir, 0);
    let mut w = writer_for(db);
    w.append(StagedRecord::new(1, 1, b"gap")).unwrap();
    w.sync().unwrap();
    drop(w);

    // Rewrite the record's LSN and fix up the record CRC so only the
    // contiguity invariant can catch it.
    let mut buf = read_page1(&path);
    {
        let bytes = buf.as_mut_slice();
        let base = PAGE_HEADER_SIZE;
        let payload_size =
            LE::read_u32(&bytes[base + RH_OFF_PAYLOAD_SIZE..base + RH_OFF_PAYLOAD_SIZE + 4])
                as usize;
        LE::write_u64(&mut bytes[base + RH_OFF_LSN..base + RH_OFF_LSN + 8], 7);
        let mut header = [0u8; RECORD_HEADER_SIZE];
        header.copy_from_slice(&bytes[base..base + RECORD_HEADER_SIZE]);
        header[40..44].fill(0); // record_crc is computed over a zeroed field
        let payload = &bytes[base + RECORD_HEADER_SIZE..base + RECORD_HEADER_SIZE + payload_size];
        let crc = RecordHeader::compute_record_crc(&header, payload);
        LE::write_u32(&mut bytes[base + 40..base + 44], crc);
    }
    {
        let page: &mut [u8; PAGE_SIZE] = buf.as_mut_slice().try_into().unwrap();
        stamp_page_crc(page);
    }
    write_page1(&path, &buf);

    let (db2, _sb) = DbFile::open(&path).unwrap();
    let err = scan_and_validate(&db2, false, |_rec| Ok(())).unwrap_err();
    assert!(
        matches!(
            err,
            ChronoError::NonMonotonicLsn { .. } | ChronoError::CorruptRecord { .. }
        ),
        "unexpected error: {err:?}"
    );
}

#[test]
fn all_zero_region_after_valid_pages_is_free_space_not_corruption() {
    let dir = tempfile::tempdir().unwrap();
    let (path, db) = fresh_db(&dir, 32);
    let mut w = writer_for(db);
    w.append(StagedRecord::new(1, 1, b"only record")).unwrap();
    w.sync().unwrap();
    drop(w);

    // The record is on page 1; pages 2..=31 stay all-zero preallocation.
    let (report, seen) = collect(&path);
    assert_eq!(report.record_count, 1);
    assert_eq!(seen[0].2, b"only record");
    assert_eq!(report.pages.len(), 1);
    assert_eq!(report.free_tail_pages, 30);
}
