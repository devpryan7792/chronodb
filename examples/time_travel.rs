//! Time travel over a ChronoDB file, end to end.
//!
//! ```sh
//! cargo run --example time_travel
//! ```
//!
//! Creates a scratch database, appends two versions of one entity, resolves
//! both with as-of reads, walks the entity's history, then reopens the file
//! and reads it again.

use chronodb::engine::{Engine, EngineOptions};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let dir = std::env::temp_dir().join("chronodb-example");
    std::fs::create_dir_all(&dir)?;
    let path = dir.join("events.cdb");
    let _ = std::fs::remove_file(&path);

    // Create the database (Engine::open reopens an existing one).
    let engine = Engine::create(&path, EngineOptions::default())?;

    // Two versions of sensor 42, stamped one second apart (nanoseconds).
    engine.append_at(42, 1_700_000_000_000_000_000, b"temperature=21.4")?;
    engine.append_at(42, 1_700_000_001_000_000_000, b"temperature=19.8")?;

    // Time travel: resolve what the entity looked like at any instant.
    let mid = engine
        .get_as_of(42, 1_700_000_000_500_000_000)?
        .expect("a version exists between the two writes");
    println!(
        "as of +0.5 s -> {} @ {} ns (lsn {})",
        String::from_utf8_lossy(mid.payload()),
        mid.timestamp_ns(),
        mid.lsn()
    );

    // Walk the whole history, oldest first.
    for rec in engine.scan_history(42, 0, u64::MAX)? {
        let rec = rec?;
        println!(
            "history     -> {} @ {} ns",
            String::from_utf8_lossy(rec.payload()),
            rec.timestamp_ns()
        );
    }

    // Durability barrier, then a clean shutdown.
    engine.sync()?;
    let stats = engine.close(true)?;
    println!(
        "closed      -> committed lsn {}, durable lsn {}, {} data pages",
        stats.committed_lsn, stats.durable_lsn, stats.pages_flushed
    );

    // ...and the file reopens with the same history.
    let engine = Engine::open(&path, EngineOptions::default())?;
    let latest = engine.get_latest(42)?.expect("reopened with history");
    println!(
        "reopened    -> {} (as of {} ns)",
        String::from_utf8_lossy(latest.payload()),
        latest.timestamp_ns()
    );
    engine.close(false)?;
    Ok(())
}
