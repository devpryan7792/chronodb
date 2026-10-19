//! End-to-end checks for `chronoctl query` against a real engine-written
//! file: as-of resolution, tombstone rendering, timestamp/id formats and
//! argument errors. The binary path comes from Cargo's `CARGO_BIN_EXE_*`.

use std::process::Command;

use chronodb::engine::{Engine, EngineOptions};
use chronodb::storage::record::REC_FLAG_TOMBSTONE;

const E: u128 = 0xC0FF_EE00_0000_0000_0000_0000_0000_0001;
const E_UUID: &str = "c0ffee00-0000-0000-0000-000000000001";
const F: u128 = 0xC0FF_EE00_0000_0000_0000_0000_0000_0002;

fn run_query(args: &[&str]) -> (i32, String, String) {
    let out = Command::new(env!("CARGO_BIN_EXE_chronoctl"))
        .arg("query")
        .args(args)
        .output()
        .expect("spawn chronoctl");
    (
        out.status.code().unwrap_or(-1),
        String::from_utf8_lossy(&out.stdout).into_owned(),
        String::from_utf8_lossy(&out.stderr).into_owned(),
    )
}

fn seeded_file() -> (tempfile::TempDir, std::path::PathBuf) {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("cli.cdb");
    let engine = Engine::create(&path, EngineOptions::default()).unwrap();
    engine.append_at(E, 1_000, b"hello").unwrap();
    engine
        .append_with(E, 2_000, b"", 0, REC_FLAG_TOMBSTONE)
        .unwrap();
    engine.append_at(F, 3_000, b"world").unwrap();
    engine.close(true).unwrap();
    (dir, path)
}

#[test]
fn query_resolves_as_of_by_unix_ns_and_rfc3339() {
    let (_dir, path) = seeded_file();
    let file = path.to_str().unwrap();

    // Before the tombstone: the entity is present with the first version.
    let (code, stdout, stderr) = run_query(&[file, "--id", E_UUID, "--as-of", "1500"]);
    assert_eq!(code, 0, "stderr: {stderr}");
    assert!(stdout.contains("state         : present"), "{stdout}");
    assert!(stdout.contains("lsn           : 1"), "{stdout}");
    assert!(stdout.contains("\"hello\""), "{stdout}");
    assert!(stdout.contains("versions seen : 1"), "{stdout}");

    // Same answer through an RFC 3339 timestamp (1.5 µs after the epoch).
    let (code, stdout, _) = run_query(&[
        file,
        "--id",
        E_UUID,
        "--as-of",
        "1970-01-01T00:00:00.0000015Z",
    ]);
    assert_eq!(code, 0);
    assert!(stdout.contains("lsn           : 1"), "{stdout}");

    // Latest resolves the tombstone: deleted, not "no record".
    let (code, stdout, _) = run_query(&[file, "--id", E_UUID]);
    assert_eq!(code, 0);
    assert!(
        stdout.contains("state         : deleted (tombstone)"),
        "{stdout}"
    );
    assert!(stdout.contains("lsn           : 2"), "{stdout}");

    // Other entities are independent; decimal ids are accepted too.
    let (code, stdout, _) = run_query(&[file, "--id", &F.to_string()]);
    assert_eq!(code, 0);
    assert!(stdout.contains("\"world\""), "{stdout}");
}

#[test]
fn query_reports_absent_records_and_usage_errors() {
    let (_dir, path) = seeded_file();
    let file = path.to_str().unwrap();

    // As-of before every record: a clean "no record", exit 0.
    let (code, stdout, _) = run_query(&[file, "--id", E_UUID, "--as-of", "999"]);
    assert_eq!(code, 0);
    assert!(
        stdout.contains("no record at or before the query time"),
        "{stdout}"
    );

    // Unknown entity behaves the same.
    let (code, stdout, _) = run_query(&[file, "--id", "0xdead"]);
    assert_eq!(code, 0);
    assert!(
        stdout.contains("no record at or before the query time"),
        "{stdout}"
    );

    // Missing/broken arguments exit with the usage code 2.
    let (code, _, stderr) = run_query(&[file]);
    assert_eq!(code, 2);
    assert!(stderr.contains("usage:"), "{stderr}");

    let (code, _, stderr) = run_query(&[file, "--id", "not-an-id"]);
    assert_eq!(code, 2);
    assert!(stderr.contains("not a uuid/hex/decimal"), "{stderr}");

    let (code, _, stderr) = run_query(&[file, "--id", E_UUID, "--as-of", "yesterday"]);
    assert_eq!(code, 2);
    assert!(stderr.contains("not an RFC 3339"), "{stderr}");
}
