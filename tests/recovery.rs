//! Phase 4 integration tests: recovery open (`Engine::open`) — validation,
//! tail truncation, index reconstruction and cursor resume.
//!
//! Every test builds a real file through the write path, damages or reopens
//! it, and then exercises the public read/write API to prove that the
//! recovered state matches the on-disk evidence.

use std::fs::{self, File, OpenOptions as FsOpenOptions};
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};

use chronodb::engine::ring::Durability;
use chronodb::engine::{Engine, EngineOptions, OpenOptions};
use chronodb::error::ChronoError;
use chronodb::storage::file::DbFile;
use chronodb::storage::page::PAGE_SIZE_U64;
use chronodb::storage::record::REC_FLAG_TOMBSTONE;
use chronodb::storage::scanner::{scan_and_validate, ScanReport};

const E: u128 = 0xC0FF_EE00_0000_0000_0000_0000_0000_0001;
const F: u128 = 0xC0FF_EE00_0000_0000_0000_0000_0002;
const G: u128 = 0xC0FF_EE00_0000_0000_0000_0000_0003;

fn db_path(dir: &tempfile::TempDir) -> PathBuf {
    dir.path().join("recover.cdb")
}

fn opts(preallocate_pages: u64) -> EngineOptions {
    EngineOptions {
        preallocate_pages,
        ..EngineOptions::default()
    }
}

/// Strict (non-repairing) scan of a file — the ground truth for tests.
fn inspect(path: &Path) -> ScanReport {
    let (db, _sb) = DbFile::open_read_only(path).expect("open read-only");
    scan_and_validate(&db, false, |_| Ok(())).expect("strict scan")
}

fn file_len(path: &Path) -> u64 {
    fs::metadata(path).expect("metadata").len()
}

/// Reads a record through the index and returns `(lsn, timestamp, payload)`.
fn resolved(engine: &Engine, entity: u128, ts: u64) -> Option<(u64, u64, Vec<u8>)> {
    engine
        .get_as_of(entity, ts)
        .unwrap()
        .map(|r| (r.header.lsn, r.header.timestamp_ns, r.payload.to_vec()))
}

// ---------------------------------------------------------------------------
// clean reopens
// ---------------------------------------------------------------------------

#[test]
fn open_on_an_empty_file_resumes_at_lsn_one() {
    let dir = tempfile::tempdir().unwrap();
    let path = db_path(&dir);
    let engine = Engine::create(&path, opts(16)).unwrap();
    engine.close(true).unwrap();

    let (engine, report) = Engine::open_with(&path, opts(16), OpenOptions::default()).unwrap();
    assert!(report.clean, "a pristine file needs no repair");
    assert_eq!(report.truncated_bytes, 0);
    assert_eq!(report.record_count, 0);
    assert_eq!(report.data_pages, 0);
    assert_eq!(report.last_lsn, 0);
    assert_eq!(report.last_ts, 0);
    assert_eq!(report.free_tail_pages, 15, "superblock plus 15 free pages");

    assert_eq!(engine.append(E, b"first").unwrap(), 1);
    engine.close(true).unwrap();

    let (engine, report) = Engine::open_with(&path, opts(16), OpenOptions::default()).unwrap();
    assert_eq!(report.record_count, 1);
    assert_eq!(report.last_lsn, 1);
    assert_eq!(
        engine.get_as_of(E, u64::MAX).unwrap().unwrap().payload(),
        b"first"
    );
    engine.close(true).unwrap();
}

#[test]
fn clean_reopen_rebuilds_the_index_and_resumes_cursors() {
    let dir = tempfile::tempdir().unwrap();
    let path = db_path(&dir);
    let engine = Engine::create(&path, opts(0)).unwrap();
    for i in 0..10u64 {
        engine
            .append_at(E, 1_000 + i * 10, format!("e-{i}").as_bytes())
            .unwrap();
    }
    for i in 0..5u64 {
        engine
            .append_at(F, 2_000 + i * 10, format!("f-{i}").as_bytes())
            .unwrap();
    }
    let stats = engine.close(true).unwrap();
    assert_eq!(stats.committed_lsn, 15);

    let (engine, report) = Engine::open_with(&path, opts(0), OpenOptions::default()).unwrap();
    assert!(report.clean);
    assert_eq!(report.record_count, 15);
    assert!(report.data_pages >= 1);
    assert_eq!(report.last_lsn, 15);
    assert_eq!(report.last_ts, 2_040);
    assert_eq!(report.free_tail_pages, 0);

    // Index fully rebuilt: as-of resolution, latest and history all work.
    assert_eq!(resolved(&engine, E, 999), None, "before every version");
    assert_eq!(
        resolved(&engine, E, 1_045),
        Some((5, 1_040, b"e-4".to_vec()))
    );
    assert_eq!(
        resolved(&engine, E, u64::MAX),
        Some((10, 1_090, b"e-9".to_vec()))
    );
    assert_eq!(
        resolved(&engine, F, 2_030),
        Some((14, 2_030, b"f-3".to_vec()))
    );
    assert_eq!(resolved(&engine, G, u64::MAX), None, "unknown entity");
    assert_eq!(engine.scan_history(F, 2_000, 2_050).unwrap().count(), 5);
    assert_eq!(engine.get_latest(F).unwrap().unwrap().payload(), b"f-4");

    // Recovered data is published immediately: no append needed to gate it.
    assert_eq!(engine.committed_lsn(), 15);
    assert_eq!(engine.durable_lsn(), 15);

    // Explicit timestamps gate on the recovered `last_ts` (2_040)…
    assert!(matches!(
        engine.append_at(E, 2_040, b"stale"),
        Err(ChronoError::NonMonotonicTimestamp { .. })
    ));
    assert_eq!(engine.append_at(E, 2_041, b"fresh").unwrap(), 16);

    // …and LSN assignment continues where the file left off, with the wall
    // clock clamped above every recovered timestamp.
    assert_eq!(engine.append(E, b"after-reopen").unwrap(), 17);
    let r = engine.get_latest(E).unwrap().unwrap();
    assert_eq!(r.payload(), b"after-reopen");
    let ts = r.header.timestamp_ns;
    assert!(ts > 2_041, "clock resumed above every accepted timestamp");
    engine.close(true).unwrap();

    let report = inspect(&path);
    assert_eq!(report.record_count, 17);
    assert_eq!(report.max_lsn, Some(17));
}

#[test]
fn repeated_reopens_keep_lsns_contiguous_and_readable() {
    let dir = tempfile::tempdir().unwrap();
    let path = db_path(&dir);

    let mut expected_lsn = 0u64;
    for round in 0..3u64 {
        let opts = opts(0);
        let engine = if round == 0 {
            Engine::create(&path, opts).unwrap()
        } else {
            Engine::open(&path, opts).unwrap()
        };
        assert_eq!(engine.committed_lsn(), expected_lsn);
        for i in 0..10u64 {
            let lsn = engine
                .append_at(
                    E,
                    10_000 * (round + 1) + i,
                    format!("r{round}-{i}").as_bytes(),
                )
                .unwrap();
            assert_eq!(lsn, expected_lsn + i + 1);
        }
        expected_lsn += 10;
        engine.close(true).unwrap();

        let report = inspect(&path);
        assert_eq!(report.record_count, expected_lsn);
        assert_eq!(report.max_lsn, Some(expected_lsn));
    }

    let (engine, report) = Engine::open_with(&path, opts(0), OpenOptions::default()).unwrap();
    assert!(report.clean);
    assert_eq!(report.record_count, 30);
    assert_eq!(engine.scan_history(E, 0, u64::MAX).unwrap().count(), 30);
    assert_eq!(
        engine.get_as_of(E, 10_005).unwrap().unwrap().payload(),
        b"r0-5"
    );
    assert_eq!(
        engine.get_as_of(E, 20_009).unwrap().unwrap().payload(),
        b"r1-9"
    );
    assert_eq!(
        engine.get_as_of(E, u64::MAX).unwrap().unwrap().payload(),
        b"r2-9"
    );
    engine.close(true).unwrap();
}

#[test]
fn reopen_recovers_tombstones_and_spanning_records() {
    let dir = tempfile::tempdir().unwrap();
    let path = db_path(&dir);
    let big = vec![0xA5u8; 10_000]; // 3 pages
    let bigger = vec![0x5Au8; 40_000]; // 10+ pages
    let engine = Engine::create(&path, opts(0)).unwrap();
    engine.append_at(E, 1_000, &big).unwrap();
    engine
        .append_with(E, 2_000, b"", 0, REC_FLAG_TOMBSTONE)
        .unwrap();
    engine.append_at(E, 3_000, &bigger).unwrap();
    engine.append_at(F, 4_000, b"small").unwrap();
    engine.close(true).unwrap();

    let strict = inspect(&path);
    assert!(strict.pages.len() >= 14, "spanning records really span");
    assert_eq!(strict.record_count, 4);

    let (engine, report) = Engine::open_with(&path, opts(0), OpenOptions::default()).unwrap();
    assert!(report.clean);
    assert_eq!(report.record_count, 4);
    assert_eq!(report.last_lsn, 4);

    // Spanning payloads are reassembled from the rebuilt index.
    assert_eq!(
        engine.get_as_of(E, 1_500).unwrap().unwrap().payload(),
        big.as_slice()
    );
    assert_eq!(
        engine.get_as_of(E, u64::MAX).unwrap().unwrap().payload(),
        bigger.as_slice()
    );
    // The tombstone hides the state as of its own timestamp...
    assert!(engine.get_as_of(E, 2_500).unwrap().is_none());
    // ...but stays in the raw history.
    let history: Vec<_> = engine
        .scan_history(E, 0, u64::MAX)
        .unwrap()
        .map(|r| r.unwrap().header.lsn)
        .collect();
    assert_eq!(history, vec![1, 2, 3]);
    assert_eq!(engine.get_latest(F).unwrap().unwrap().payload(), b"small");

    // The resumed writer appends further spanning records correctly.
    let more = vec![0x11u8; 12_345];
    assert_eq!(engine.append_at(G, 5_000, &more).unwrap(), 5);
    assert_eq!(
        engine.get_as_of(G, u64::MAX).unwrap().unwrap().payload(),
        more.as_slice()
    );
    engine.close(true).unwrap();

    let (engine, report) = Engine::open_with(&path, opts(0), OpenOptions::default()).unwrap();
    assert!(report.clean);
    assert_eq!(report.record_count, 5);
    assert_eq!(report.last_lsn, 5);
    assert_eq!(report.last_ts, 5_000);
    assert_eq!(
        engine.get_as_of(G, u64::MAX).unwrap().unwrap().payload(),
        more.as_slice()
    );
    engine.close(true).unwrap();
}

#[test]
fn abandon_keeps_the_durable_prefix_and_recovery_is_monotonic() {
    let dir = tempfile::tempdir().unwrap();
    let path = db_path(&dir);
    let mut opts = opts(0);
    opts.channel.durability = Durability::OnSync;

    let engine = Engine::create(&path, opts.clone()).unwrap();
    for i in 0..20u64 {
        engine.append_at(E, 1_000 + i, &[i as u8; 64]).unwrap();
    }
    assert_eq!(engine.sync().unwrap(), 20, "first 20 records are durable");
    for i in 0..20u64 {
        engine.append_at(E, 2_000 + i, &[7u8; 64]).unwrap();
    }
    engine.abandon().unwrap(); // no final fdatasync

    let (engine, report) = Engine::open_with(&path, opts.clone(), OpenOptions::default()).unwrap();
    assert!(
        report.last_lsn >= 20 && report.last_lsn <= 40,
        "durable prefix survives; unflushed tail may or may not: {}",
        report.last_lsn
    );
    // Every recovered record is readable and matches its expected payload.
    let records: Vec<(u64, Vec<u8>)> = engine
        .scan_history(E, 0, u64::MAX)
        .unwrap()
        .map(|r| {
            let r = r.unwrap();
            (r.header.timestamp_ns, r.payload.to_vec())
        })
        .collect();
    assert_eq!(records.len() as u64, report.last_lsn);
    for (ts, payload) in &records {
        let expected: Vec<u8> = if *ts < 2_000 {
            vec![(ts - 1_000) as u8; 64]
        } else {
            vec![7u8; 64]
        };
        assert_eq!(payload, &expected, "record at ts {ts}");
    }
    assert_eq!(
        engine.append(E, b"post-recovery").unwrap(),
        report.last_lsn + 1
    );
    let stats = engine.close(true).unwrap();
    assert_eq!(stats.committed_lsn, report.last_lsn + 1);

    // A second recovery sees exactly the same prefix plus the new record.
    let (engine, report2) = Engine::open_with(&path, opts, OpenOptions::default()).unwrap();
    assert!(report2.clean);
    assert_eq!(report2.record_count, report.record_count + 1);
    assert_eq!(report2.last_lsn, report.last_lsn + 1);
    assert_eq!(
        engine.get_latest(E).unwrap().unwrap().payload(),
        b"post-recovery"
    );
    engine.close(true).unwrap();
}

// ---------------------------------------------------------------------------
// torn / corrupt tails
// ---------------------------------------------------------------------------

/// Appends `n` records of `len` bytes to `entity` as one batch: one batch is
/// one worker iteration and one durability step, so records pack densely
/// into pages and only the final page stays partially filled. Returns the
/// assigned LSNs (contiguous, in submission order).
fn append_packed(engine: &Engine, entity: u128, n: usize, len: usize) -> Vec<u64> {
    let payloads: Vec<Vec<u8>> = (0..n).map(|i| vec![i as u8; len]).collect();
    let batch: Vec<(u128, &[u8])> = payloads.iter().map(|p| (entity, p.as_slice())).collect();
    engine.append_batch(&batch).expect("batch append")
}

/// Payload of the `lsn`-th record of an [`append_packed`] stream.
fn packed_payload(lsn: u64, len: usize) -> Vec<u8> {
    vec![(lsn - 1) as u8; len]
}

/// Reads the whole entity back and returns `(lsn, payload)` pairs in order.
fn read_back(engine: &Engine, entity: u128) -> Vec<(u64, Vec<u8>)> {
    engine
        .scan_history(entity, 0, u64::MAX)
        .unwrap()
        .map(|r| {
            let r = r.unwrap();
            (r.header.lsn, r.payload.to_vec())
        })
        .collect()
}

#[test]
fn a_torn_trailing_page_is_truncated_and_the_prefix_survives() {
    let dir = tempfile::tempdir().unwrap();
    let path = db_path(&dir);
    let engine = Engine::create(&path, opts(0)).unwrap();
    append_packed(&engine, E, 40, 300);
    engine.close(true).unwrap();

    let strict = inspect(&path);
    assert!(strict.pages.len() >= 3, "need a multi-page sealed prefix");
    assert!(
        strict.pages[0].record_count > 1,
        "batched records share pages (not one page per record)"
    );
    let sealed_bytes = (strict.pages.len() as u64 + 1) * PAGE_SIZE_U64;
    let last_page_index = strict.pages.last().unwrap().page_index;
    assert_eq!(file_len(&path), sealed_bytes, "no pre-allocation slack");

    // Simulate the power-loss window: a half-written page beyond the sealed
    // prefix — plausible header (correct epoch, CRC left stale) plus garbage.
    let mut torn = vec![0u8; PAGE_SIZE_U64 as usize];
    torn[..8].copy_from_slice(&(last_page_index + 1).to_le_bytes());
    torn[20..24].copy_from_slice(&0xDEAD_BEEFu32.to_le_bytes()); // wrong CRC
    for (i, b) in torn.iter_mut().enumerate().skip(32) {
        *b = (i as u8).wrapping_mul(0x9D);
    }
    let file: File = FsOpenOptions::new().write(true).open(&path).unwrap();
    file.write_all_at(&torn, sealed_bytes).unwrap();
    file.sync_all().unwrap();
    drop(file);
    assert_eq!(file_len(&path), sealed_bytes + PAGE_SIZE_U64);

    let (engine, report) = Engine::open_with(&path, opts(0), OpenOptions::default()).unwrap();
    assert!(!report.clean);
    assert_eq!(
        report.truncated_bytes, PAGE_SIZE_U64,
        "exactly the torn page"
    );
    assert_eq!(report.last_lsn, 40, "every sealed record survives");
    assert_eq!(report.record_count, 40);
    assert_eq!(file_len(&path), sealed_bytes, "file cut back to the prefix");

    // Fully readable, and the next append resumes at the right LSN.
    let records = read_back(&engine, E);
    assert_eq!(records.len(), 40);
    for (lsn, payload) in records {
        assert_eq!(payload, packed_payload(lsn, 300), "record {lsn}");
    }
    assert_eq!(engine.append(E, b"resumed").unwrap(), 41);
    engine.close(true).unwrap();

    // A strict reopen of the repaired file is clean again.
    let strict = inspect(&path);
    assert_eq!(strict.record_count, 41);
    assert_eq!(strict.max_lsn, Some(41));
    assert_eq!(strict.truncated_bytes, None);
}

#[test]
fn a_bit_flip_in_the_last_sealed_page_drops_only_that_page() {
    let dir = tempfile::tempdir().unwrap();
    let path = db_path(&dir);
    let engine = Engine::create(&path, opts(0)).unwrap();
    append_packed(&engine, E, 40, 300);
    engine.close(true).unwrap();

    let strict = inspect(&path);
    let pages = strict.pages.clone();
    assert!(pages.len() >= 3);
    let last = *pages.last().unwrap();
    let keep = pages[pages.len() - 2];
    let keep_records: u64 = pages[..pages.len() - 1]
        .iter()
        .map(|p| u64::from(p.record_count))
        .sum();

    // Media corruption: one flipped bit inside the payload area of the last
    // sealed page — its CRC no longer matches.
    let mut bytes = fs::read(&path).unwrap();
    let bit = (last.page_index as usize) * PAGE_SIZE_U64 as usize + (last.used_bytes - 1);
    bytes[bit] ^= 0x20;
    fs::write(&path, &bytes).unwrap();

    let (engine, report) = Engine::open_with(&path, opts(0), OpenOptions::default()).unwrap();
    assert!(!report.clean);
    assert_eq!(report.truncated_bytes, PAGE_SIZE_U64);
    assert_eq!(report.last_lsn, keep.max_lsn, "only the damaged page went");
    assert_eq!(report.record_count, keep_records);
    assert_eq!(file_len(&path), last.page_index as u64 * PAGE_SIZE_U64);

    let records = read_back(&engine, E);
    assert_eq!(records.len() as u64, keep_records);
    for (lsn, payload) in records {
        assert_eq!(payload, packed_payload(lsn, 300), "record {lsn}");
    }
    assert_eq!(
        engine.append(E, b"after-bit-flip").unwrap(),
        keep.max_lsn + 1
    );
    engine.close(true).unwrap();

    let strict = inspect(&path);
    assert_eq!(strict.max_lsn, Some(keep.max_lsn + 1));
}

#[test]
fn a_partial_page_tail_is_cut_during_recovery() {
    let dir = tempfile::tempdir().unwrap();
    let path = db_path(&dir);
    let engine = Engine::create(&path, opts(0)).unwrap();
    append_packed(&engine, E, 60, 300);
    engine.close(true).unwrap();

    let strict = inspect(&path);
    let data_pages = strict.pages.len() as u64;
    assert!(data_pages >= 2);
    let total = file_len(&path);
    assert_eq!(total, (data_pages + 1) * PAGE_SIZE_U64);
    // Cut inside the last data page: the residue is shorter than a page.
    let cut_to = total - 777;
    let file: File = FsOpenOptions::new().write(true).open(&path).unwrap();
    file.set_len(cut_to).unwrap();
    file.sync_all().unwrap();
    drop(file);

    let (engine, report) = Engine::open_with(&path, opts(0), OpenOptions::default()).unwrap();
    assert!(!report.clean);
    assert_eq!(report.truncated_bytes, PAGE_SIZE_U64 - 777);
    assert_eq!(report.data_pages, data_pages - 1);
    assert_eq!(
        report.last_lsn,
        strict.pages[data_pages as usize - 2].max_lsn
    );
    assert_eq!(
        file_len(&path),
        data_pages * PAGE_SIZE_U64,
        "page-aligned again"
    );
    assert_eq!(engine.append(E, b"realigned").unwrap(), report.last_lsn + 1);
    engine.close(true).unwrap();

    let strict = inspect(&path);
    assert_eq!(strict.max_lsn, Some(report.last_lsn + 1));
}

#[test]
fn strict_open_rejects_corruption_without_touching_the_file() {
    let dir = tempfile::tempdir().unwrap();
    let path = db_path(&dir);
    let engine = Engine::create(&path, opts(0)).unwrap();
    append_packed(&engine, E, 40, 256);
    engine.close(true).unwrap();

    let strict = inspect(&path);
    let pages = strict.pages.clone();
    assert!(pages.len() >= 3);
    let last = *pages.last().unwrap();
    let keep = pages[pages.len() - 2];
    let keep_records: u64 = pages[..pages.len() - 1]
        .iter()
        .map(|p| u64::from(p.record_count))
        .sum();

    // One flipped byte inside the last page's record area.
    let mut bytes = fs::read(&path).unwrap();
    let off = (last.page_index as usize) * PAGE_SIZE_U64 as usize + last.used_bytes - 1;
    bytes[off] ^= 0xFF;
    fs::write(&path, &bytes).unwrap();
    let len_before = bytes.len() as u64;

    let err = Engine::open_with(&path, opts(0), OpenOptions { repair: false })
        .err()
        .expect("strict open must reject a corrupt page");
    assert!(
        matches!(err, ChronoError::PageChecksumMismatch { .. }),
        "unexpected error: {err:?}"
    );
    assert_eq!(fs::read(&path).unwrap(), bytes, "file must be untouched");
    assert_eq!(file_len(&path), len_before);

    // The repairing open recovers the intact prefix instead: the damaged
    // page is gone, everything before it is intact.
    let (engine, report) = Engine::open_with(&path, opts(0), OpenOptions::default()).unwrap();
    assert!(!report.clean);
    assert_eq!(report.last_lsn, keep.max_lsn);
    assert_eq!(report.record_count, keep_records);
    let records = read_back(&engine, E);
    assert_eq!(records.len() as u64, keep_records);
    for (lsn, payload) in records {
        assert_eq!(payload, packed_payload(lsn, 256), "record {lsn}");
    }
    engine.close(true).unwrap();
}

#[test]
fn preallocated_tail_pages_are_reused_after_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let path = db_path(&dir);
    let preallocated = 64 * PAGE_SIZE_U64;

    let engine = Engine::create(&path, opts(64)).unwrap();
    for i in 0..5u64 {
        engine.append_at(E, 1_000 + i, &[i as u8; 128]).unwrap();
    }
    engine.close(true).unwrap();
    assert_eq!(file_len(&path), preallocated);

    let (engine, report) = Engine::open_with(&path, opts(64), OpenOptions::default()).unwrap();
    assert!(report.clean);
    assert!(
        report.free_tail_pages > 0,
        "pre-allocated slack is still free"
    );

    // The resumed writer lands inside the pre-allocated region.
    for i in 0..5u64 {
        engine.append_at(F, 2_000 + i, &[0xEE; 128]).unwrap();
    }
    engine.close(true).unwrap();
    assert_eq!(file_len(&path), preallocated, "no growth beyond the slack");

    let (engine, report) = Engine::open_with(&path, opts(64), OpenOptions::default()).unwrap();
    assert!(report.clean);
    assert_eq!(report.record_count, 10);
    assert_eq!(report.last_lsn, 10);
    assert_eq!(report.last_ts, 2_004);
    assert_eq!(
        engine.get_latest(F).unwrap().unwrap().payload(),
        &[0xEE; 128][..]
    );
    assert_eq!(
        engine.get_as_of(E, 1_004).unwrap().unwrap().payload(),
        &[4u8; 128][..]
    );
    engine.close(true).unwrap();
}
