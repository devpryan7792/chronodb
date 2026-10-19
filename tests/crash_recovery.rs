//! Phase 5: the power-loss simulator (SPEC §7 Phase 5).
//!
//! Two adversarial harnesses share this file:
//!
//! * **SIGKILL harness** (`sigkill_never_loses_acknowledged_records`) — spawns
//!   a *child process* that appends records with [`Durability::PerBatch`]
//!   (default) and prints one `ACK <lsn>` line per record once its
//!   `fdatasync` completed, then kills it with `SIGKILL` mid-flight and
//!   reopens the file. Every acknowledged LSN must survive, the recovered
//!   history must be a contiguous prefix with byte-exact payloads, and the
//!   engine must keep accepting appends. Three kill/recover rounds.
//!
//!   The child is this very test binary, re-executed with
//!   `--exact crash_child_writer`; that test is a no-op in a normal run and
//!   only turns into the writer when `CHRONODB_CRASH_DB` names a database.
//!
//! * **Torn-write fuzz** (`randomized_tail_damage_recovery_is_sound`) —
//!   takes a healthy file and, for each of many deterministic seeds, cuts it
//!   at a random non-page boundary and flips random bytes in the tail. A
//!   repairing `Engine::open` must then recover a *prefix* of the original
//!   records with byte-exact payloads, leave a page-aligned strictly-valid
//!   file, and keep working for further appends.

use std::collections::BTreeMap;
use std::fs;
use std::io::{BufRead, BufReader, Read};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};

use chronodb::engine::{Engine, EngineOptions, OpenOptions};
use chronodb::storage::file::DbFile;
use chronodb::storage::page::PAGE_SIZE_U64;
use chronodb::storage::scanner::{scan_and_validate, ScanReport};

const E: u128 = 0xC0FF_EE00_0000_0000_0000_0000_0000_00AA;
const CHILD_ENV: &str = "CHRONODB_CRASH_DB";
/// Acknowledged appends per kill round.
const ACKS_PER_ROUND: usize = 75;
const ROUNDS: usize = 3;
/// Fuzz iterations for the torn-write harness.
const FUZZ_CASES: usize = 120;

fn opts() -> EngineOptions {
    EngineOptions {
        preallocate_pages: 0,
        ..EngineOptions::default()
    }
}

/// The deterministic payload of record `lsn`: length varies per LSN (every
/// fourth record spans pages), the first eight bytes hold the LSN, the rest
/// is a position-dependent byte pattern.
fn expected_payload(lsn: u64) -> Vec<u8> {
    let len = 64 + (lsn % 4) as usize * 3000;
    let mut payload = vec![0u8; len];
    payload[..8].copy_from_slice(&lsn.to_le_bytes());
    for (i, b) in payload[8..].iter_mut().enumerate() {
        *b = (lsn as u8).wrapping_mul(31).wrapping_add(i as u8);
    }
    payload
}

/// Strict (non-repairing) scan of a healthy file.
fn strict_scan(path: &Path) -> ScanReport {
    let (db, _sb) = DbFile::open_read_only(path).expect("open read-only");
    scan_and_validate(&db, false, |_| Ok(())).expect("strict scan")
}

/// Reads an entity back through the recovered engine as `(lsn, payload)`.
fn recovered(engine: &Engine) -> Vec<(u64, Vec<u8>)> {
    engine
        .scan_history(E, 0, u64::MAX)
        .expect("history scan")
        .map(|r| {
            let r = r.expect("record read (CRC-checked)");
            (r.header.lsn, r.payload.to_vec())
        })
        .collect()
}

/// Asserts that `records` is the contiguous prefix `1..=n` of the original
/// stream with byte-exact payloads, and returns `n`.
fn assert_prefix(records: &[(u64, Vec<u8>)], context: &str) -> u64 {
    for (i, (lsn, payload)) in records.iter().enumerate() {
        assert_eq!(
            *lsn,
            i as u64 + 1,
            "{context}: LSN prefix must be contiguous"
        );
        assert_eq!(
            payload,
            &expected_payload(*lsn),
            "{context}: payload of lsn {lsn}"
        );
    }
    records.len() as u64
}

// ---------------------------------------------------------------------------
// the child writer (same binary, re-executed with --exact)
// ---------------------------------------------------------------------------

/// Child side of the SIGKILL harness. In a normal test run it returns
/// immediately; the parent runs it via `--exact crash_child_writer` with
/// [`CHILD_ENV`] set, and it then appends durable records forever, printing
/// one `ACK <lsn>` line per completed `fdatasync` — until the parent's
/// `SIGKILL` lands in the middle of the next append.
#[test]
fn crash_child_writer() {
    let Ok(db) = std::env::var(CHILD_ENV) else {
        return;
    };
    let path = PathBuf::from(db);
    let engine = if path.exists() {
        Engine::open(&path, opts()).expect("child: recovery open")
    } else {
        Engine::create(&path, opts()).expect("child: create")
    };
    let mut next = engine.committed_lsn() + 1;
    loop {
        let lsn = engine
            .append(E, &expected_payload(next))
            .expect("child: append");
        assert_eq!(lsn, next, "child: LSN must continue the recovered prefix");
        // Line-buffered stdout: reaching the parent proves the ack.
        println!("ACK {lsn}");
        next += 1;
    }
}

fn spawn_child(path: &Path) -> Child {
    Command::new(std::env::current_exe().expect("current exe"))
        .args(["--exact", "crash_child_writer", "--nocapture"])
        .env(CHILD_ENV, path)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn child writer")
}

// ---------------------------------------------------------------------------
// SIGKILL harness
// ---------------------------------------------------------------------------

#[test]
fn sigkill_never_loses_acknowledged_records() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("crash.cdb");
    let mut acked: Vec<u64> = Vec::new();

    for round in 0..ROUNDS {
        // --- kill a live writer mid-append --------------------------------
        let mut child = spawn_child(&path);
        let stdout = child.stdout.take().expect("child stdout");
        let mut reader = BufReader::new(stdout);
        let mut line = String::new();
        let mut acked_this_round = 0usize;
        while acked_this_round < ACKS_PER_ROUND {
            line.clear();
            let n = reader.read_line(&mut line).expect("read ack line");
            assert!(
                n > 0,
                "round {round}: child exited before {ACKS_PER_ROUND} acks (acks so far: {:?})",
                acked.len()
            );
            // Ignore libtest's own output; the first ack shares a line with
            // its `test crash_child_writer ... ` prefix.
            if let Some(pos) = line.find("ACK ") {
                if let Ok(lsn) = line[pos + 4..].trim().parse::<u64>() {
                    acked.push(lsn);
                    acked_this_round += 1;
                }
            }
        }
        child.kill().expect("SIGKILL");
        let status = child.wait().expect("reap child");
        assert_eq!(
            status.code(),
            None,
            "round {round}: child must die by signal, not exit normally"
        );
        let mut stderr = String::new();
        child
            .stderr
            .take()
            .expect("child stderr")
            .read_to_string(&mut stderr)
            .expect("read child stderr");
        assert!(stderr.is_empty(), "round {round}: child stderr: {stderr}");

        // --- recover and verify -------------------------------------------
        let (engine, report) = Engine::open_with(&path, opts(), OpenOptions::default())
            .unwrap_or_else(|e| panic!("round {round}: recovery failed: {e:?}"));
        let records = recovered(&engine);
        let context = format!("round {round}");
        let max = assert_prefix(&records, &context);
        assert_eq!(report.last_lsn, max, "{context}: report/scan agreement");
        for lsn in &acked {
            assert!(
                *lsn <= max,
                "{context}: acknowledged lsn {lsn} was lost (recovered prefix ends at {max})"
            );
        }

        // The recovered engine is a working engine: one more durable record,
        // then a clean close with a page-aligned, strictly-valid file.
        let lsn = engine
            .append(E, &expected_payload(max + 1))
            .expect("append after recovery");
        assert_eq!(lsn, max + 1, "{context}: LSN resume");
        assert_eq!(engine.sync().unwrap(), max + 1);
        engine.close(true).unwrap();
        acked.push(max + 1);

        let strict = strict_scan(&path);
        assert_eq!(strict.max_lsn, Some(max + 1), "{context}: strict scan");
        assert_eq!(
            fs::metadata(&path).unwrap().len() % PAGE_SIZE_U64,
            0,
            "{context}: file must stay page-aligned"
        );
    }

    assert!(
        acked.len() >= ROUNDS * ACKS_PER_ROUND,
        "the harness really acknowledged records"
    );
}

// ---------------------------------------------------------------------------
// randomized torn-write fuzz
// ---------------------------------------------------------------------------

/// xorshift64* — deterministic damage so failures reproduce from the seed.
struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.0 = x;
        x.wrapping_mul(0x2545_F491_4F6C_DD1D)
    }

    fn below(&mut self, n: u64) -> u64 {
        self.next() % n.max(1)
    }
}

#[test]
fn randomized_tail_damage_recovery_is_sound() {
    let dir = tempfile::tempdir().unwrap();
    let base = dir.path().join("base.cdb");
    let engine = Engine::create(&base, opts()).unwrap();
    for lsn in 1..=120u64 {
        engine
            .append_at(E, 1_000_000 + lsn, &expected_payload(lsn))
            .unwrap();
    }
    engine.close(true).unwrap();

    // Ground truth: the pristine file, scanned strictly.
    let original: BTreeMap<u64, Vec<u8>> = {
        let (db, _sb) = DbFile::open_read_only(&base).unwrap();
        let mut map = BTreeMap::new();
        scan_and_validate(&db, false, |v| {
            map.insert(v.lsn(), v.payload.to_vec());
            Ok(())
        })
        .unwrap();
        map
    };
    assert_eq!(original.len(), 120);

    let mut rng = Rng(0x9E37_79B9_7F4A_7C15);
    for case in 0..FUZZ_CASES {
        let path = dir.path().join(format!("damaged-{case}.cdb"));
        fs::copy(&base, &path).unwrap();

        // Damage variant: tail cut + tail flips, a bare page-boundary cut, or
        // flips anywhere in the data region — all three are real crash /
        // media-damage shapes.
        let mut bytes = fs::read(&path).unwrap();
        let len = bytes.len() as u64;
        match case % 3 {
            0 => {
                let cut = PAGE_SIZE_U64 + rng.below(len - PAGE_SIZE_U64);
                bytes.truncate(cut as usize);
                let tail_start = (cut - 3 * PAGE_SIZE_U64).max(PAGE_SIZE_U64);
                let flips = 1 + rng.below(6);
                for _ in 0..flips {
                    let at = tail_start + rng.below(cut - tail_start);
                    bytes[at as usize] ^= 1u8 << rng.below(8);
                }
            }
            1 => {
                // Page-aligned cut (whole pages missing, nothing partial).
                let pages = len / PAGE_SIZE_U64;
                let keep = 1 + rng.below(pages - 1);
                bytes.truncate((keep * PAGE_SIZE_U64) as usize);
            }
            _ => {
                // Flips anywhere past the superblock, file length unchanged.
                let flips = 1 + rng.below(8);
                for _ in 0..flips {
                    let at = PAGE_SIZE_U64 + rng.below(len - PAGE_SIZE_U64);
                    bytes[at as usize] ^= 1u8 << rng.below(8);
                }
            }
        }
        fs::write(&path, &bytes).unwrap();

        // Recovery must succeed and be sound: a byte-exact prefix of the
        // original records, nothing invented, nothing reordered.
        let (engine, report) = Engine::open_with(&path, opts(), OpenOptions::default())
            .unwrap_or_else(|e| panic!("case {case}: recovery failed: {e:?}"));
        let records = recovered(&engine);
        let context = format!("case {case}");
        let max = assert_prefix(&records, &context);
        assert_eq!(report.last_lsn, max, "{context}: report/scan agreement");
        assert!(
            max <= 120,
            "{context}: cannot recover more than was written"
        );
        for (lsn, payload) in &records {
            assert_eq!(
                Some(payload),
                original.get(lsn),
                "{context}: recovered lsn {lsn} differs from the original"
            );
        }
        assert_eq!(
            fs::metadata(&path).unwrap().len() % PAGE_SIZE_U64,
            0,
            "{context}: repaired file must be page-aligned"
        );

        // The repaired file is a working database: append, close, and pass a
        // strict verification.
        let lsn = engine.append(E, &expected_payload(max + 1)).unwrap();
        assert_eq!(lsn, max + 1, "{context}: LSN resume");
        engine.close(true).unwrap();
        let strict = strict_scan(&path);
        assert_eq!(strict.max_lsn, Some(max + 1), "{context}: strict scan");
        assert_eq!(strict.truncated_bytes, None, "{context}: repaired clean");
    }
}

/// A spanning record whose continuation pages never reached disk leaves its
/// first-fragment page dangling: repair must cut the pages the record
/// touched, resume the LSN there and reuse the first page from scratch.
#[test]
fn an_in_flight_spanning_record_is_cut_with_its_pages() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("inflight.cdb");
    let engine = Engine::create(&path, opts()).unwrap();
    // lsn 7 is a spanning record (payload 9064 bytes over three pages);
    // 8 % 4 == 0 would be a tiny record.
    for lsn in 1..=7u64 {
        engine
            .append_at(E, 1_000 + lsn, &expected_payload(lsn))
            .unwrap();
    }
    engine.close(true).unwrap();

    // Where does the last (spanning) record start?
    let start_page = {
        let (db, _sb) = DbFile::open_read_only(&path).unwrap();
        let mut start = 0;
        scan_and_validate(&db, false, |v| {
            if v.lsn() == 7 {
                start = v.start_page;
            }
            Ok(())
        })
        .unwrap();
        assert!(start > 1, "record 7 must start past the first page");
        start
    };
    let full_pages = strict_scan(&path).pages.len() as u64 + 1;
    assert!(
        start_page < full_pages,
        "record 7 must really span multiple pages"
    );

    // Power loss right after the record's first fragment page reached disk:
    // its continuation pages are simply absent.
    let file = fs::OpenOptions::new().write(true).open(&path).unwrap();
    file.set_len((start_page + 1) * PAGE_SIZE_U64).unwrap();
    file.sync_all().unwrap();
    drop(file);

    let (engine, report) = Engine::open_with(&path, opts(), OpenOptions::default()).unwrap();
    assert!(!report.clean);
    assert_eq!(
        report.truncated_bytes, PAGE_SIZE_U64,
        "the dangling first fragment is cut with the record"
    );
    assert_eq!(report.last_lsn, 6, "only completed records survive");
    assert_eq!(report.record_count, 6);
    assert_eq!(
        fs::metadata(&path).unwrap().len(),
        start_page * PAGE_SIZE_U64,
        "cut back to the record's first page"
    );
    let records = recovered(&engine);
    assert_eq!(assert_prefix(&records, "in-flight record"), 6);

    // The writer reuses page `start_page` for record 7; the file stays
    // strictly valid afterwards.
    assert_eq!(engine.append_at(E, 9_000, &expected_payload(7)).unwrap(), 7);
    engine.close(true).unwrap();
    let strict = strict_scan(&path);
    assert_eq!(strict.max_lsn, Some(7));
    assert_eq!(strict.truncated_bytes, None);
}

/// A flipped superblock is never repaired silently: recovery must fail with
/// `CorruptSuperblock` before touching the file (SPEC §5.2 step 1).
#[test]
fn superblock_damage_refuses_to_open() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("superblock.cdb");
    let engine = Engine::create(&path, opts()).unwrap();
    engine.append_at(E, 1_000, b"keep me").unwrap();
    engine.close(true).unwrap();

    let mut bytes = fs::read(&path).unwrap();
    bytes[8] ^= 0x80; // magic or CRC region of page 0
    fs::write(&path, &bytes).unwrap();

    let err = Engine::open_with(&path, opts(), OpenOptions::default())
        .err()
        .expect("superblock damage must refuse to open");
    assert!(
        matches!(err, chronodb::error::ChronoError::CorruptSuperblock { .. }),
        "unexpected error: {err:?}"
    );
    assert_eq!(fs::read(&path).unwrap(), bytes, "file must be untouched");
}
