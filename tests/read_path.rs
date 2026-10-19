//! Phase 3 integration tests: mmap + sparse index read path — as-of
//! resolution, latest lookup, history scans, zero-copy stability across
//! remaps, CRC verification and concurrent append/query traffic.
//!
//! All tests exercise the public API (`Engine`) end to end: create a file,
//! append through the SPSC channel, resolve through the index.

use std::os::unix::fs::FileExt;

use chronodb::engine::{ChronoEngine, Engine, EngineOptions};
use chronodb::error::ChronoError;
use chronodb::storage::page::PAGE_SIZE_U64;
use chronodb::storage::record::{RECORD_HEADER_SIZE, REC_FLAG_TOMBSTONE, RH_OFF_PAYLOAD_SIZE};

const E: u128 = 0xC0FF_EE00_0000_0000_0000_0000_0000_0001;
const F: u128 = 0xC0FF_EE00_0000_0000_0000_0000_0002;

fn temp_engine(preallocate_pages: u64) -> (tempfile::TempDir, Engine) {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("read.cdb");
    let opts = EngineOptions {
        preallocate_pages,
        ..EngineOptions::default()
    };
    let engine = Engine::create(&path, opts).expect("create engine");
    (dir, engine)
}

#[test]
fn as_of_resolves_the_latest_version_at_or_before_the_timestamp() {
    let (_dir, engine) = temp_engine(0);
    assert_eq!(engine.append_at(E, 1_000, b"v1").unwrap(), 1);
    assert_eq!(engine.append_at(E, 2_000, b"v2").unwrap(), 2);
    assert_eq!(engine.append_at(E, 3_000, b"v3").unwrap(), 3);
    assert_eq!(engine.append_at(F, 3_500, b"other").unwrap(), 4);

    let payload = |ts: u64| {
        engine
            .get_as_of(E, ts)
            .unwrap()
            .map(|r| (r.header.lsn, r.header.timestamp_ns, r.payload.to_vec()))
    };

    assert_eq!(payload(999), None, "before the first version");
    assert_eq!(
        payload(1_000),
        Some((1, 1_000, b"v1".to_vec())),
        "exact hit"
    );
    assert_eq!(payload(1_999), Some((1, 1_000, b"v1".to_vec())));
    assert_eq!(payload(2_000), Some((2, 2_000, b"v2".to_vec())));
    assert_eq!(payload(2_999), Some((2, 2_000, b"v2".to_vec())));
    assert_eq!(payload(3_000), Some((3, 3_000, b"v3".to_vec())));
    assert_eq!(payload(u64::MAX), Some((3, 3_000, b"v3".to_vec())));

    // Payloads point straight into the mapping: the entity header matches.
    let r = engine.get_as_of(E, 5_000).unwrap().unwrap();
    let eid = r.header.entity_id;
    assert_eq!(eid, E);

    // Unknown entity resolves to None, not an error.
    assert!(engine.get_as_of(0xDEAD_BEEF, u64::MAX).unwrap().is_none());
}

#[test]
fn tombstones_hide_the_state_but_stay_in_history() {
    let (_dir, engine) = temp_engine(0);
    engine.append_at(E, 1_000, b"alive").unwrap();
    engine
        .append_with(E, 2_000, b"", 0, REC_FLAG_TOMBSTONE)
        .unwrap();

    assert_eq!(
        engine.get_as_of(E, 1_999).unwrap().unwrap().payload(),
        b"alive"
    );
    assert!(
        engine.get_as_of(E, 2_000).unwrap().is_none(),
        "tombstone resolves to None"
    );

    // `get_latest` keeps raw visibility: the tombstone is the newest record.
    let latest = engine
        .get_latest(E)
        .unwrap()
        .expect("latest is the tombstone");
    assert!(latest.header.is_tombstone());
    let tombstone_ts = latest.header.timestamp_ns;
    assert_eq!(tombstone_ts, 2_000);

    // Resurrection: a later record becomes the newest state again.
    engine.append_at(E, 3_000, b"reborn").unwrap();
    assert_eq!(
        engine.get_as_of(E, u64::MAX).unwrap().unwrap().payload(),
        b"reborn"
    );
    assert!(!engine.get_latest(E).unwrap().unwrap().header.is_tombstone());

    // History includes the tombstone and stays ordered.
    let hist: Vec<_> = engine
        .scan_history(E, 0, u64::MAX)
        .unwrap()
        .map(|r| {
            let r = r.unwrap();
            (
                r.header.timestamp_ns,
                r.header.is_tombstone(),
                r.payload.to_vec(),
            )
        })
        .collect();
    assert_eq!(
        hist,
        vec![
            (1_000, false, b"alive".to_vec()),
            (2_000, true, Vec::new()),
            (3_000, false, b"reborn".to_vec()),
        ]
    );
}

#[test]
fn get_latest_returns_the_newest_committed_record() {
    let (_dir, engine) = temp_engine(0);
    assert!(engine.get_latest(E).unwrap().is_none());
    engine.append_at(E, 10, b"a").unwrap();
    engine.append_at(E, 20, b"b").unwrap();
    let latest = engine.get_latest(E).unwrap().unwrap();
    assert_eq!((latest.header.lsn, latest.payload.as_ref()), (2, &b"b"[..]));
    assert!(engine.get_latest(F).unwrap().is_none());
}

#[test]
fn scan_history_bounds_are_inclusive_and_ordered() {
    let (_dir, engine) = temp_engine(0);
    for (i, ts) in (100u64..=600).step_by(100).enumerate() {
        engine.append_at(E, ts, format!("v{i}").as_bytes()).unwrap();
    }

    let ts_of = |from, to| {
        engine
            .scan_history(E, from, to)
            .unwrap()
            .map(|r| r.unwrap().header.timestamp_ns)
            .collect::<Vec<_>>()
    };
    assert_eq!(ts_of(0, u64::MAX), vec![100, 200, 300, 400, 500, 600]);
    assert_eq!(
        ts_of(200, 500),
        vec![200, 300, 400, 500],
        "inclusive bounds"
    );
    assert_eq!(ts_of(200, 200), vec![200]);
    assert_eq!(ts_of(201, 299), Vec::<u64>::new());
    assert_eq!(ts_of(700, 800), Vec::<u64>::new());
    assert_eq!(ts_of(600, 100), Vec::<u64>::new(), "reversed range");

    // Snapshot semantics: later appends do not mutate an existing iterator.
    let mut iter = engine.scan_history(E, 0, u64::MAX).unwrap();
    assert_eq!(iter.remaining(), 6);
    engine.append_at(E, 700, b"v6").unwrap();
    assert_eq!(iter.len(), 6);
    let first_ts = iter.next().unwrap().unwrap().header.timestamp_ns;
    assert_eq!(first_ts, 100);

    // Lazy dereference: dropped iterators cost nothing, exhausted ones
    // yield every record.
    let all = engine
        .scan_history(E, 0, u64::MAX)
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(all.len(), 7);
    assert!(all.windows(2).all(|w| w[0].header.lsn <= w[1].header.lsn));
}

#[test]
fn zero_copy_refs_stay_valid_when_the_file_grows() {
    let (_dir, engine) = temp_engine(0);
    // 4000 bytes fit in one page (4064-byte payload area minus the 44-byte
    // header), so this record is served as a zero-copy mapping slice.
    let needle = vec![0x5Au8; 4_000];
    engine.append_at(E, 1_000, &needle).unwrap();

    let before = engine.get_as_of(E, 1_000).unwrap().unwrap();
    assert!(matches!(before.payload, std::borrow::Cow::Borrowed(_)));
    let ptr = before.payload.as_ptr();
    assert_eq!(before.payload(), needle.as_slice());

    // Grow the file well past the first mapping's extent so the manager
    // adds regions; these 64 KiB records each span many pages.
    let blob = vec![0xA5u8; 64 * 1024];
    let batch: Vec<(u128, &[u8])> = (0..160u64)
        .map(|i| (0xB000 + i as u128, blob.as_slice()))
        .collect();
    let lsns = engine.append_batch(&batch).unwrap();
    assert_eq!(lsns.len(), 160);
    assert!(
        engine.mmap().region_count() >= 2,
        "appends grew past the first mapping (regions: {})",
        engine.mmap().region_count()
    );

    // The earlier record's slice must be byte-identical at the same address.
    let after = engine.get_as_of(E, 1_000).unwrap().unwrap();
    assert_eq!(after.payload(), needle.as_slice());
    assert_eq!(after.payload.as_ptr(), ptr, "mapping must never move");

    // Records from the grown tail are readable too — and because page
    // headers interrupt a spanning payload on disk, they are reassembled.
    let tail = engine.get_as_of(0xB000 + 159, u64::MAX).unwrap().unwrap();
    assert!(matches!(tail.payload, std::borrow::Cow::Owned(_)));
    assert_eq!(tail.payload().len(), blob.len());
    assert!(tail.payload().iter().all(|&b| b == 0xA5));

    // Boundary: the largest payload that still fits one page is zero-copy…
    let exact = vec![0x11u8; 4_020];
    engine.append(F, &exact).unwrap();
    let r = engine.get_latest(F).unwrap().unwrap();
    assert!(matches!(r.payload, std::borrow::Cow::Borrowed(_)));
    assert_eq!(r.payload(), exact.as_slice());

    // …one byte more makes it span and reassemble.
    let spanning = vec![0x22u8; 4_021];
    engine.append(F, &spanning).unwrap();
    let r = engine.get_latest(F).unwrap().unwrap();
    assert!(matches!(r.payload, std::borrow::Cow::Owned(_)));
    assert_eq!(r.payload(), spanning.as_slice());
}

#[test]
fn corrupt_payload_is_caught_by_record_crc() {
    let (_dir, engine) = temp_engine(0);
    engine.append_at(E, 1_000, b"pristine-payload").unwrap();
    assert!(engine.get_as_of(E, 1_000).unwrap().is_some());

    // First record: page 1, byte 32 (after the page header), payload after
    // the 44-byte record header.
    let payload_at = PAGE_SIZE_U64 + 32 + RECORD_HEADER_SIZE as u64;
    let f = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(engine.path())
        .unwrap();
    let mut original = [0u8; 1];
    f.read_exact_at(&mut original, payload_at).unwrap();
    let corrupt = [original[0] ^ 0xFF];
    f.write_all_at(&corrupt, payload_at).unwrap();

    match engine.get_as_of(E, 1_000) {
        Err(ChronoError::RecordChecksumMismatch { lsn: 1 }) => {}
        other => panic!("expected RecordChecksumMismatch, got {other:?}"),
    }
    // The consumer-facing iterator surfaces the same failure per record.
    let first = engine.scan_history(E, 0, u64::MAX).unwrap().next().unwrap();
    assert!(matches!(
        first,
        Err(ChronoError::RecordChecksumMismatch { .. })
    ));
}

#[test]
fn readers_race_appends_without_torn_state() {
    let (_dir, engine) = temp_engine(0);
    const N: u64 = 400;
    const BASE_TS: u64 = 1_000_000;

    std::thread::scope(|s| {
        s.spawn(|| {
            for i in 0..N {
                engine.append_at(E, BASE_TS + i, &i.to_le_bytes()).unwrap();
            }
        });

        // Hammer the read path while the writer runs: every observation
        // must be a whole, consistent version (exact payload<->stamp
        // pairing, no torn bytes).
        let mut last_seen = 0u64;
        while engine.committed_lsn() < N {
            for r in [
                engine.get_as_of(E, u64::MAX).unwrap(),
                engine.get_latest(E).unwrap(),
            ]
            .into_iter()
            .flatten()
            {
                let eid = r.header.entity_id;
                assert_eq!(eid, E);
                let i = u64::from_le_bytes(r.payload().try_into().expect("8-byte payload"));
                assert!(i < N);
                let ts = r.header.timestamp_ns;
                assert_eq!(ts, BASE_TS + i);
                assert_eq!(r.payload(), i.to_le_bytes());
                last_seen = last_seen.max(i);
            }
            std::hint::spin_loop();
        }
        // After the writer committed everything, the newest version is up.
        let last = engine.get_latest(E).unwrap().unwrap();
        last_seen = last_seen.max(u64::from_le_bytes(last.payload().try_into().unwrap()));
        assert_eq!(last_seen, N - 1);

        // History scans during/after the run are consistent and complete.
        let hist: Vec<u64> = engine
            .scan_history(E, 0, u64::MAX)
            .unwrap()
            .map(|r| u64::from_le_bytes(r.unwrap().payload().try_into().unwrap()))
            .collect();
        assert_eq!(hist, (0..N).collect::<Vec<_>>());
    });

    let final_ref = engine.get_as_of(E, u64::MAX).unwrap().unwrap();
    assert_eq!(
        u64::from_le_bytes(final_ref.payload().try_into().unwrap()),
        N - 1
    );
}

#[test]
fn engine_is_usable_through_the_chrono_engine_trait() {
    let (_dir, engine) = temp_engine(0);
    let e: &dyn ChronoEngine = &engine;
    let lsn = e.append(E, b"via-trait").unwrap();
    assert_eq!(lsn, 1);
    assert_eq!(e.sync().unwrap(), 1);
    let r = e.get_as_of(E, u64::MAX).unwrap().unwrap();
    assert_eq!(r.payload(), b"via-trait");
    let trait_lsn = e.get_latest(E).unwrap().unwrap().header.lsn;
    assert_eq!(trait_lsn, 1);
    let history: Vec<_> = e.scan_history(E, 0, u64::MAX).unwrap().collect();
    assert_eq!(history.len(), 1);
    assert_eq!(e.append_batch(&[(E, b"x")]).unwrap().len(), 1);
    assert_eq!(engine.durable_lsn(), 2);
}

#[test]
fn uncommitted_suffix_never_masks_older_versions() {
    // The worker indexes a batch before it publishes `committed_lsn`, so a
    // momentary suffix of uncommitted entries exists in every batch; the
    // racing readers above exercise that window. This test pins the
    // ordering invariant deterministically by planting a synthetic entry
    // with an out-of-this-world LSN: queries must ignore it and still
    // resolve the newest *committed* version.
    let (_dir, engine) = temp_engine(0);
    engine.append_at(E, 1_000, b"v1").unwrap();
    engine.append_at(E, 2_000, b"v2").unwrap();

    engine.index().insert(
        E,
        chronodb::index::SparseIndexEntry {
            timestamp_ns: 2_500,
            lsn: 100,       // > committed_lsn (2): indexed but not yet published
            file_offset: 0, // never dereferenced: the LSN gate filters it
        },
    );

    assert_eq!(engine.index().entry_count(), 3, "entry is indexed…");
    let at_2500 = engine.get_as_of(E, 2_500).unwrap().unwrap();
    assert_eq!(at_2500.payload(), b"v2", "…but the gate keeps it invisible");
    let masked_ts = at_2500.header.timestamp_ns;
    assert_eq!(masked_ts, 2_000);
    assert_eq!(engine.get_latest(E).unwrap().unwrap().payload(), b"v2");
}

#[test]
fn oversized_payloads_are_rejected_without_index_pollution() {
    let (_dir, engine) = temp_engine(0);
    let big = vec![7u8; (1 << 20) + 1];
    let err = engine.append_at(E, 1_000, &big).unwrap_err();
    assert!(matches!(err, ChronoError::RecordTooLarge { .. }));
    assert!(engine.get_latest(E).unwrap().is_none());
    assert_eq!(engine.index().entry_count(), 0);
    assert_eq!(engine.committed_lsn(), 0);
}

#[test]
fn index_counts_match_the_append_log() {
    let (_dir, engine) = temp_engine(0);
    engine.append_at(E, 10, b"a").unwrap();
    engine.append_at(F, 20, b"b").unwrap();
    engine.append_at(E, 30, b"c").unwrap();
    assert_eq!(engine.index().entity_count(), 2);
    assert_eq!(engine.index().entry_count(), 3);
    assert!(engine.mmap().mapped_len() >= PAGE_SIZE_U64 * 2);
}

/// Guards the payload-size field used by the corruption test above: if the
/// header layout ever changes, the computed offset breaks loudly here.
#[test]
fn record_header_layout_assumption() {
    assert_eq!(RH_OFF_PAYLOAD_SIZE, 32);
    assert_eq!(RECORD_HEADER_SIZE, 44);
}
