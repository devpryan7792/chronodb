//! Phase 2 integration tests: the append-only write path end to end — the
//! `Engine` facade, the SPSC ring, the single-writer worker, ack semantics
//! under both durability policies, batching, concurrency and reopen.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use chronodb::engine::ring::{ChannelOptions, Durability};
use chronodb::engine::{Engine, EngineOptions, EngineStats};
use chronodb::error::ChronoError;
use chronodb::storage::file::DbFile;
use chronodb::storage::record::REC_FLAG_TOMBSTONE;
use chronodb::storage::scanner::scan_and_validate;

fn open_engine(dir: &tempfile::TempDir, name: &str, opts: EngineOptions) -> (PathBuf, Engine) {
    let path = dir.path().join(name);
    let engine = Engine::create(&path, opts).expect("engine create");
    (path, engine)
}

fn onsync_opts() -> EngineOptions {
    EngineOptions {
        channel: ChannelOptions {
            durability: Durability::OnSync,
            ..ChannelOptions::default()
        },
        ..EngineOptions::default()
    }
}

/// One scanned record: `(lsn, entity_id, payload, timestamp_ns)`.
type ScanRow = (u64, u128, Vec<u8>, u64);

/// Full validated scan of a closed database.
fn scan(path: &Path) -> (u64, Vec<ScanRow>) {
    let (db, _sb) = DbFile::open(path).unwrap();
    let mut rows = Vec::new();
    let report = scan_and_validate(&db, false, |rec| {
        rows.push((
            rec.lsn(),
            rec.entity_id(),
            rec.payload.to_vec(),
            rec.timestamp_ns(),
        ));
        Ok(())
    })
    .unwrap();
    (report.record_count, rows)
}

/// `prev_page_lsn`/epoch sanity for every data page.
fn assert_page_chain(path: &Path) {
    let (db, _sb) = DbFile::open(path).unwrap();
    let report = scan_and_validate(&db, false, |_rec| Ok(())).unwrap();
    for (i, p) in report.pages.iter().enumerate() {
        assert_eq!(p.page_epoch, i as u64 + 1);
        assert!(p.is_closed());
    }
    assert_eq!(report.pages[0].prev_page_lsn, 0);
    for w in report.pages.windows(2) {
        assert_eq!(w[1].prev_page_lsn, w[0].max_lsn);
    }
}

#[test]
fn appends_are_durable_and_reopen_in_order() {
    let dir = tempfile::tempdir().unwrap();
    let (path, engine) = open_engine(&dir, "a.cdb", EngineOptions::default());

    let mut expected = Vec::new();
    for i in 0..200u64 {
        let payload = vec![(i % 251) as u8; (i % 97) as usize + 1];
        let lsn = engine.append(i as u128, &payload).unwrap();
        assert_eq!(lsn, i + 1);
        expected.push((i as u128, payload));
    }
    // PerBatch (the default) acknowledges only after fdatasync.
    assert_eq!(engine.committed_lsn(), 200);
    assert_eq!(engine.durable_lsn(), 200);

    let stats: EngineStats = engine.close(true).unwrap();
    assert_eq!(stats.committed_lsn, 200);
    assert_eq!(stats.durable_lsn, 200);

    let (count, rows) = scan(&path);
    assert_eq!(count, 200);
    assert_eq!(rows.len(), 200);
    for (i, (lsn, entity, payload, _ts)) in rows.iter().enumerate() {
        assert_eq!(*lsn, i as u64 + 1);
        assert_eq!(*entity, expected[i].0);
        assert_eq!(*payload, expected[i].1);
    }
    let timestamps: Vec<u64> = rows.iter().map(|r| r.3).collect();
    assert!(
        timestamps.windows(2).all(|w| w[0] <= w[1]),
        "timestamps must be non-decreasing in LSN order"
    );
    assert_page_chain(&path);
}

#[test]
fn explicit_sync_barrier_reports_durable_lsn() {
    let dir = tempfile::tempdir().unwrap();
    let (path, engine) = open_engine(&dir, "b.cdb", onsync_opts());

    for i in 0..50u64 {
        assert_eq!(engine.append(i as u128, b"payload").unwrap(), i + 1);
    }
    // OnSync: the ack means "pages written", not "fdatasync done".
    assert_eq!(engine.committed_lsn(), 50);
    assert_eq!(engine.durable_lsn(), 0);

    let durable = engine.sync().unwrap();
    assert_eq!(durable, 50);
    assert!(engine.durable_lsn() >= 50);

    let stats = engine.close(true).unwrap();
    assert_eq!(stats.durable_lsn, 50);
    let (count, _) = scan(&path);
    assert_eq!(count, 50);
}

#[test]
fn abandon_skips_final_sync_but_pages_reach_the_page_cache() {
    let dir = tempfile::tempdir().unwrap();
    let (path, engine) = open_engine(&dir, "c.cdb", onsync_opts());

    for i in 0..3u64 {
        engine.append(i as u128, &vec![0x33u8; 2000]).unwrap();
    }
    let stats = engine.abandon().unwrap();
    assert_eq!(stats.committed_lsn, 3);
    assert_eq!(stats.durable_lsn, 0, "no fdatasync ever ran");

    // The pages were still written to the page cache, so they are visible on
    // reopen — abandoning only skips the sync, never loses packed pages.
    let (count, rows) = scan(&path);
    assert_eq!(count, 3);
    assert_eq!(rows[0].2, vec![0x33u8; 2000]);
}

#[test]
fn explicit_timestamps_must_strictly_increase() {
    let dir = tempfile::tempdir().unwrap();
    let (path, engine) = open_engine(&dir, "d.cdb", EngineOptions::default());

    assert_eq!(engine.append_at(1, 5_000, b"a").unwrap(), 1);
    let err = engine.append_at(1, 5_000, b"b").unwrap_err();
    assert!(matches!(
        err,
        ChronoError::NonMonotonicTimestamp {
            previous: 5_000,
            current: 5_000
        }
    ));
    let err = engine.append_at(1, 4_999, b"c").unwrap_err();
    assert!(matches!(
        err,
        ChronoError::NonMonotonicTimestamp {
            previous: 5_000,
            current: 4_999
        }
    ));
    // Rejections consume neither timestamps nor LSNs.
    assert_eq!(engine.append_at(1, 5_001, b"d").unwrap(), 2);
    engine.close(true).unwrap();

    let (count, rows) = scan(&path);
    assert_eq!(count, 2);
    assert_eq!(rows[0].3, 5_000);
    assert_eq!(rows[1].3, 5_001);
}

#[test]
fn oversized_records_are_rejected_without_killing_the_engine() {
    let dir = tempfile::tempdir().unwrap();
    let (path, engine) = open_engine(&dir, "e.cdb", EngineOptions::default());

    let huge = vec![0u8; (1 << 20) + 1];
    let err = engine.append(7, &huge).unwrap_err();
    assert!(
        matches!(err, ChronoError::RecordTooLarge { .. }),
        "unexpected error: {err:?}"
    );

    assert_eq!(engine.append(7, b"ok-1").unwrap(), 1);
    assert_eq!(engine.append(7, b"ok-2").unwrap(), 2);
    let stats = engine.close(true).unwrap();
    assert_eq!(stats.committed_lsn, 2);

    let (count, rows) = scan(&path);
    assert_eq!(count, 2);
    assert_eq!(rows[0].2, b"ok-1");
    assert_eq!(rows[1].2, b"ok-2");
}

#[test]
fn concurrent_appenders_get_unique_contiguous_lsns() {
    const THREADS: u64 = 4;
    const PER_THREAD: u64 = 250;
    let dir = tempfile::tempdir().unwrap();
    let (path, engine) = open_engine(&dir, "f.cdb", EngineOptions::default());
    let engine = Arc::new(engine);

    let mut handles = Vec::new();
    for t in 0..THREADS {
        let engine = Arc::clone(&engine);
        handles.push(std::thread::spawn(move || {
            let payload = [t as u8; 32];
            let mut lsns = Vec::with_capacity(PER_THREAD as usize);
            for _ in 0..PER_THREAD {
                lsns.push(engine.append(t as u128, &payload).unwrap());
            }
            lsns
        }));
    }

    let mut all = Vec::new();
    for h in handles {
        all.extend(h.join().unwrap());
    }
    all.sort_unstable();
    assert_eq!(all, (1..=THREADS * PER_THREAD).collect::<Vec<u64>>());

    let engine = Arc::try_unwrap(engine).map_err(|_| ()).unwrap();
    assert_eq!(engine.committed_lsn(), THREADS * PER_THREAD);
    let stats = engine.close(true).unwrap();
    assert_eq!(stats.durable_lsn, THREADS * PER_THREAD);

    let (count, rows) = scan(&path);
    assert_eq!(count, THREADS * PER_THREAD);
    let timestamps: Vec<u64> = rows.iter().map(|r| r.3).collect();
    assert!(timestamps.windows(2).all(|w| w[0] <= w[1]));
}

#[test]
fn append_batch_returns_ordered_lsns_and_scan_matches() {
    let dir = tempfile::tempdir().unwrap();
    let (path, engine) = open_engine(&dir, "g.cdb", EngineOptions::default());

    assert!(engine.append_batch(&[]).unwrap().is_empty());

    let payloads: Vec<Vec<u8>> = (0..300usize)
        .map(|i| vec![(i % 256) as u8; 16 + i % 50])
        .collect();
    let batch: Vec<(u128, &[u8])> = payloads
        .iter()
        .enumerate()
        .map(|(i, p)| (i as u128, p.as_slice()))
        .collect();
    let lsns = engine.append_batch(&batch).unwrap();
    assert_eq!(lsns, (1..=300).collect::<Vec<u64>>());

    engine.close(true).unwrap();

    let (count, rows) = scan(&path);
    assert_eq!(count, 300);
    for (i, (lsn, entity, payload, _)) in rows.iter().enumerate() {
        assert_eq!(*lsn, i as u64 + 1);
        assert_eq!(*entity, i as u128);
        assert_eq!(payload, &payloads[i]);
    }
    assert_page_chain(&path);
}

#[test]
fn small_ring_and_ack_slab_survive_pressure() {
    let dir = tempfile::tempdir().unwrap();
    let opts = EngineOptions {
        channel: ChannelOptions {
            ring_capacity: 4096, // about 16 in-flight frames
            ack_slab: 64,        // forces slab wraparound
            ..ChannelOptions::default()
        },
        ..EngineOptions::default()
    };
    let (path, engine) = open_engine(&dir, "h.cdb", opts);
    let engine = Arc::new(engine);

    let mut handles = Vec::new();
    for t in 0..2u64 {
        let engine = Arc::clone(&engine);
        handles.push(std::thread::spawn(move || {
            for i in 0..500u64 {
                engine.append(t as u128, &[i as u8; 200]).unwrap();
            }
        }));
    }
    for h in handles {
        h.join().unwrap();
    }

    let engine = Arc::try_unwrap(engine).map_err(|_| ()).unwrap();
    assert_eq!(engine.committed_lsn(), 1000);
    engine.close(true).unwrap();

    let (count, _) = scan(&path);
    assert_eq!(count, 1000);
}

#[test]
fn spanning_records_roundtrip_through_the_engine() {
    let dir = tempfile::tempdir().unwrap();
    let (path, engine) = open_engine(&dir, "i.cdb", EngineOptions::default());

    let big: Vec<u8> = (0..600_000u32).map(|i| (i % 251) as u8).collect();
    assert_eq!(engine.append(0xFEED, &big).unwrap(), 1);
    assert_eq!(engine.append(0xFEED, b"tail").unwrap(), 2);
    engine.close(true).unwrap();

    let (db, _sb) = DbFile::open(&path).unwrap();
    let mut spans = Vec::new();
    let report = scan_and_validate(&db, false, |rec| {
        spans.push((rec.lsn(), rec.payload.to_vec(), rec.pages_spanned));
        Ok(())
    })
    .unwrap();
    assert_eq!(report.record_count, 2);
    assert_eq!(spans[0].0, 1);
    assert_eq!(spans[0].1, big);
    assert!(spans[0].2 > 100, "600 KB must span ~150 pages");
    assert_eq!(spans[1], (2, b"tail".to_vec(), 1));
}

#[test]
fn tombstones_roundtrip_through_the_engine() {
    let dir = tempfile::tempdir().unwrap();
    let (path, engine) = open_engine(&dir, "j.cdb", EngineOptions::default());

    assert_eq!(engine.append_with(0xAB, 10, b"v1", 0, 0).unwrap(), 1);
    assert_eq!(
        engine
            .append_with(0xAB, 20, b"", 0, REC_FLAG_TOMBSTONE)
            .unwrap(),
        2
    );
    engine.close(true).unwrap();

    let (db, _sb) = DbFile::open(&path).unwrap();
    let mut rows = Vec::new();
    scan_and_validate(&db, false, |rec| {
        rows.push((rec.lsn(), rec.is_tombstone(), rec.payload.to_vec()));
        Ok(())
    })
    .unwrap();
    assert_eq!(
        rows,
        vec![(1, false, b"v1".to_vec()), (2, true, Vec::new())]
    );
}

#[test]
fn drop_without_close_finalises_durably() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("k.cdb");
    {
        let engine = Engine::create(&path, EngineOptions::default()).unwrap();
        for i in 0..5u64 {
            assert_eq!(engine.append(i as u128, b"dropped").unwrap(), i + 1);
        }
        // `engine` is dropped here: best-effort close with final sync.
    }
    let (count, rows) = scan(&path);
    assert_eq!(count, 5);
    assert!(rows.iter().all(|r| r.2 == b"dropped"));
}
