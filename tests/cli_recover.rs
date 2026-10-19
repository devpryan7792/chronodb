//! End-to-end checks for `chronoctl recover` (the SPEC §5.2 boot path):
//! the tool validates the file, cuts a torn tail, reports the recovery log
//! line and leaves a file that a strict `verify` accepts again.

use std::fs::OpenOptions as FsOpenOptions;
use std::os::unix::fs::FileExt;
use std::process::Command;

use chronodb::engine::{Engine, EngineOptions};
use chronodb::storage::page::PAGE_SIZE_U64;

fn run(args: &[&str]) -> (i32, String, String) {
    let out = Command::new(env!("CARGO_BIN_EXE_chronoctl"))
        .args(args)
        .output()
        .expect("spawn chronoctl");
    (
        out.status.code().unwrap_or(-1),
        String::from_utf8_lossy(&out.stdout).into_owned(),
        String::from_utf8_lossy(&out.stderr).into_owned(),
    )
}

#[test]
fn recover_cuts_a_torn_tail_and_reports_the_boot_state() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("recover.cdb");
    let file = path.to_str().unwrap();

    // 30 packed records via the engine, then a clean close.
    let engine = Engine::create(
        &path,
        EngineOptions {
            preallocate_pages: 0,
            ..EngineOptions::default()
        },
    )
    .unwrap();
    let payloads: Vec<Vec<u8>> = (0..30).map(|i| vec![i as u8; 300]).collect();
    let batch: Vec<(u128, &[u8])> = payloads.iter().map(|p| (7u128, p.as_slice())).collect();
    engine.append_batch(&batch).unwrap();
    engine.close(true).unwrap();

    let bytes = std::fs::read(&path).unwrap();
    let sealed = bytes.len() as u64;

    // Torn page beyond the sealed prefix (as after a power loss).
    let mut torn = vec![0u8; PAGE_SIZE_U64 as usize];
    for (i, b) in torn.iter_mut().enumerate().skip(32) {
        *b = (i as u8).wrapping_mul(0x3B);
    }
    let f: std::fs::File = FsOpenOptions::new().write(true).open(&path).unwrap();
    f.write_all_at(&torn, sealed).unwrap();
    f.sync_all().unwrap();
    drop(f);

    let (code, stdout, stderr) = run(&["recover", file]);
    assert_eq!(code, 0, "stderr: {stderr}");
    assert!(
        stdout.contains("RECOVER: Truncated 4096 corrupt uncommitted bytes at file tail"),
        "{stdout}"
    );
    assert!(stdout.contains("records         : 30"), "{stdout}");
    assert!(stdout.contains("resume lsn      : 31"), "{stdout}");
    assert_eq!(std::fs::metadata(&path).unwrap().len(), sealed);

    // The repaired file passes a strict verification and a second boot.
    let (code, stdout, stderr) = run(&["verify", file]);
    assert_eq!(code, 0, "stderr: {stderr}");
    assert!(stdout.contains("ok — 30 records"), "{stdout}");

    let (code, stdout, _) = run(&["recover", file]);
    assert_eq!(code, 0);
    assert!(
        stdout.contains("RECOVER: clean file — no truncation needed"),
        "{stdout}"
    );

    // A genuinely clean file boots cleanly without touching anything.
    let clean = dir.path().join("clean.cdb");
    let engine = Engine::create(&clean, EngineOptions::default()).unwrap();
    engine.append_at(1, 42, b"boot").unwrap();
    engine.close(true).unwrap();
    let before = std::fs::read(&clean).unwrap();
    let (code, stdout, _) = run(&["recover", clean.to_str().unwrap()]);
    assert_eq!(code, 0);
    assert!(
        stdout.contains("RECOVER: clean file — no truncation needed"),
        "{stdout}"
    );
    assert!(stdout.contains("resume lsn      : 2"), "{stdout}");
    assert_eq!(std::fs::read(&clean).unwrap(), before, "file untouched");

    // Missing file / bad usage exit non-zero.
    let (code, _, _) = run(&["recover"]);
    assert_eq!(code, 2);
    let (code, _, stderr) = run(&["recover", "/nonexistent/nope.cdb"]);
    assert_eq!(code, 1);
    assert!(!stderr.is_empty());
}
