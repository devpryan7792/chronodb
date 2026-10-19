//! `chronodb` — micro-benchmark harness.
//!
//! ```text
//! chronodb bench [--file PATH] [--records N] [--payload BYTES] [--threads T]
//! chronodb readbench [--file PATH] [--records N] [--entities M] [--payload BYTES]
//!                    [--queries Q] [--latest L] [--scans S] [--seed S]
//!                    [--out-dir DIR] [--json PATH]
//! ```
//!
//! * `bench` measures the write path (Phase 2): creates a fresh database,
//!   appends `N` records of `BYTES` payload bytes (optionally from `T`
//!   concurrent appenders), times the appends, the durability barrier and a
//!   clean close, and reports throughput and terminal LSNs.
//! * `readbench` (Phase 5) builds a dataset of `N` records spread over `M`
//!   entities and measures read latency with percentiles for three workloads
//!   — as-of point queries, latest lookups and per-entity history scans — then
//!   writes the dataset and the exact query sets to TSV files so an external
//!   harness (e.g. `bench/compare_read.py`) can replay them against other
//!   engines (SQLite, DuckDB). The payload of record `i` is deterministic
//!   (`payload_of(i, entity)`), so replayers can regenerate it byte-for-byte.

use std::env;
use std::fs::{self, File};
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};
use std::process::ExitCode;
use std::sync::Arc;
use std::time::Instant;

use chronodb::engine::{Engine, EngineOptions};
use chronodb::storage::file::DbFile;
use chronodb::storage::scanner::scan_and_validate;

const WRITE_USAGE: &str = "usage: chronodb bench [--file PATH] [--records N] [--payload BYTES] \
                           [--threads T]";
const READ_USAGE: &str = "usage: chronodb readbench [--file PATH] [--records N] [--entities M] \
                          [--payload BYTES] [--queries Q] [--latest L] [--scans S] [--seed S] \
                          [--out-dir DIR] [--json PATH]";

/// Records per `append_batch` call during the readbench write phase (one
/// worker iteration, one durability step, densely packed pages).
const WRITE_BATCH: usize = 4096;

fn main() -> ExitCode {
    let args: Vec<String> = env::args().skip(1).collect();
    match args.first().map(String::as_str) {
        Some("bench") => dispatch(&args[1..], parse_write_args, run_write, WRITE_USAGE),
        Some("readbench") => dispatch(&args[1..], parse_read_args, run_read, READ_USAGE),
        _ => {
            eprintln!("{WRITE_USAGE}");
            eprintln!("{READ_USAGE}");
            ExitCode::from(2)
        }
    }
}

fn dispatch<T>(
    args: &[String],
    parse: fn(&[String]) -> Result<T, String>,
    run: fn(&T) -> Result<(), Box<dyn std::error::Error>>,
    usage: &str,
) -> ExitCode {
    let parsed = match parse(args) {
        Ok(a) => a,
        Err(msg) => {
            eprintln!("chronodb: {msg}");
            eprintln!("{usage}");
            return ExitCode::from(2);
        }
    };
    match run(&parsed) {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("chronodb: {e}");
            ExitCode::FAILURE
        }
    }
}

// ---------------------------------------------------------------------------
// write bench (Phase 2)
// ---------------------------------------------------------------------------

struct BenchArgs {
    file: PathBuf,
    records: u64,
    payload: usize,
    threads: usize,
}

fn parse_write_args(args: &[String]) -> Result<BenchArgs, String> {
    let mut parsed = BenchArgs {
        file: PathBuf::from("bench.cdb"),
        records: 100_000,
        payload: 128,
        threads: 1,
    };
    let mut it = args.iter();
    while let Some(flag) = it.next() {
        let value = it
            .next()
            .ok_or_else(|| format!("`{flag}` needs a value"))?
            .as_str();
        match flag.as_str() {
            "--file" => parsed.file = PathBuf::from(value),
            "--records" => {
                parsed.records = value.parse().map_err(|_| "bad --records".to_string())?;
            }
            "--payload" => {
                parsed.payload = value.parse().map_err(|_| "bad --payload".to_string())?;
            }
            "--threads" => {
                parsed.threads = value.parse().map_err(|_| "bad --threads".to_string())?;
                if parsed.threads == 0 {
                    return Err("--threads must be >= 1".to_string());
                }
            }
            other => return Err(format!("unknown flag `{other}`")),
        }
    }
    Ok(parsed)
}

fn run_write(args: &BenchArgs) -> Result<(), Box<dyn std::error::Error>> {
    if args.file.exists() {
        return Err(format!(
            "refusing to overwrite existing file {} (bench always starts fresh)",
            args.file.display()
        )
        .into());
    }
    if args.payload > (1 << 20) {
        return Err("--payload exceeds the 1 MiB record cap".into());
    }

    // Records smaller than the payload cap never span pages; that keeps the
    // throughput number honest for the common case.
    let payload = vec![0xA5u8; args.payload];
    let engine = Arc::new(Engine::create(&args.file, EngineOptions::default())?);

    println!(
        "chronodb bench: {} records x {} B payload, {} thread(s), file {}",
        args.records,
        args.payload,
        args.threads,
        args.file.display()
    );

    let start = Instant::now();
    let mut handles = Vec::new();
    for t in 0..args.threads {
        let engine = Arc::clone(&engine);
        let payload = payload.clone();
        let share = args.records / args.threads as u64
            + u64::from((t as u64) < args.records % args.threads as u64);
        handles.push(std::thread::spawn(move || -> Result<Vec<u64>, String> {
            let mut lsns = Vec::with_capacity(share as usize);
            for _ in 0..share {
                lsns.push(
                    engine
                        .append(0xC0FFEE, &payload)
                        .map_err(|e| e.to_string())?,
                );
            }
            Ok(lsns)
        }));
    }
    let mut total = 0u64;
    let mut min_lsn = u64::MAX;
    let mut max_lsn = 0u64;
    for h in handles {
        let lsns = h
            .join()
            .map_err(|_| "an appender thread panicked".to_string())??;
        total += lsns.len() as u64;
        for lsn in lsns {
            min_lsn = min_lsn.min(lsn);
            max_lsn = max_lsn.max(lsn);
        }
    }
    let elapsed = start.elapsed();

    let sync_start = Instant::now();
    let durable = engine.sync()?;
    let sync_elapsed = sync_start.elapsed();

    let stats = Arc::try_unwrap(engine)
        .map_err(|_| "appender threads outlived the bench")?
        .close(true)?;

    let secs = elapsed.as_secs_f64();
    let per_sec = total as f64 / secs.max(f64::MIN_POSITIVE);
    let mb_s = (total as f64 * args.payload as f64) / secs.max(f64::MIN_POSITIVE) / 1e6;
    println!("appended        : {total} records in {:.3} s", secs);
    println!("throughput      : {per_sec:.0} records/s, {mb_s:.1} MB/s");
    println!(
        "lsn range       : {}..={}",
        if min_lsn == u64::MAX { 0 } else { min_lsn },
        max_lsn
    );
    println!(
        "sync barrier    : durable_lsn {} in {:.3} ms",
        durable,
        sync_elapsed.as_secs_f64() * 1e3
    );
    println!(
        "close           : committed {} durable {} data pages {}",
        stats.committed_lsn, stats.durable_lsn, stats.pages_flushed
    );
    let bytes = std::fs::metadata(&args.file)?.len();
    println!("file            : {} bytes", bytes);
    Ok(())
}

// ---------------------------------------------------------------------------
// read bench (Phase 5)
// ---------------------------------------------------------------------------

struct ReadArgs {
    file: PathBuf,
    records: u64,
    entities: u64,
    payload: usize,
    queries: u64,
    latest: u64,
    scans: u64,
    seed: u64,
    out_dir: Option<PathBuf>,
    json: Option<PathBuf>,
}

fn parse_read_args(args: &[String]) -> Result<ReadArgs, String> {
    let mut parsed = ReadArgs {
        file: PathBuf::from("readbench.cdb"),
        records: 200_000,
        entities: 1_000,
        payload: 64,
        queries: 50_000,
        latest: 20_000,
        scans: 2_000,
        seed: 0x5EED,
        out_dir: None,
        json: None,
    };
    let mut it = args.iter();
    while let Some(flag) = it.next() {
        let value = it
            .next()
            .ok_or_else(|| format!("`{flag}` needs a value"))?
            .as_str();
        match flag.as_str() {
            "--file" => parsed.file = PathBuf::from(value),
            "--out-dir" => parsed.out_dir = Some(PathBuf::from(value)),
            "--json" => parsed.json = Some(PathBuf::from(value)),
            "--records" => {
                parsed.records = value.parse().map_err(|_| "bad --records".to_string())?;
                if parsed.records == 0 {
                    return Err("--records must be >= 1".to_string());
                }
            }
            "--entities" => {
                parsed.entities = value.parse().map_err(|_| "bad --entities".to_string())?;
                if parsed.entities == 0 {
                    return Err("--entities must be >= 1".to_string());
                }
            }
            "--payload" => {
                parsed.payload = value.parse().map_err(|_| "bad --payload".to_string())?;
                if parsed.payload > (1 << 20) {
                    return Err("--payload exceeds the 1 MiB record cap".to_string());
                }
            }
            "--queries" => {
                parsed.queries = value.parse().map_err(|_| "bad --queries".to_string())?;
                if parsed.queries == 0 {
                    return Err("--queries must be >= 1".to_string());
                }
            }
            "--latest" => {
                parsed.latest = value.parse().map_err(|_| "bad --latest".to_string())?;
            }
            "--scans" => {
                parsed.scans = value.parse().map_err(|_| "bad --scans".to_string())?;
            }
            "--seed" => {
                parsed.seed = value.parse().map_err(|_| "bad --seed".to_string())?;
            }
            other => return Err(format!("unknown flag `{other}`")),
        }
    }
    Ok(parsed)
}

/// xorshift64 — deterministic query generation (replayed by the harness
/// through the emitted TSV files, so the two sides always agree).
struct Rng(u64);

impl Rng {
    fn new(seed: u64) -> Self {
        Self(seed | 1)
    }

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

/// The deterministic payload of record `index` (its LSN minus one) for
/// `entity_id`: byte `k` is `(index * 31 + entity + k) & 0xFF`. Replayers
/// regenerate the same bytes from the emitted dataset.
fn payload_of(index: u64, entity_id: u128, len: usize) -> Vec<u8> {
    let entity = entity_id as u64;
    (0..len as u64)
        .map(|k| (index.wrapping_mul(31).wrapping_add(entity).wrapping_add(k) & 0xFF) as u8)
        .collect()
}

/// Latency distribution of one workload, in nanoseconds.
struct Latency {
    n: u64,
    hits: u64,
    mean_ns: u64,
    p50_ns: u64,
    p90_ns: u64,
    p99_ns: u64,
    p999_ns: u64,
    max_ns: u64,
}

impl Latency {
    fn from_samples(mut samples: Vec<u64>, hits: u64) -> Self {
        samples.sort_unstable();
        let n = samples.len() as u64;
        let at = |p: f64| -> u64 {
            if samples.is_empty() {
                return 0;
            }
            let idx = ((samples.len() as f64 * p).ceil() as usize).clamp(1, samples.len()) - 1;
            samples[idx]
        };
        let sum: u128 = samples.iter().map(|s| u128::from(*s)).sum();
        Self {
            n,
            hits,
            mean_ns: if n == 0 {
                0
            } else {
                (sum / u128::from(n)) as u64
            },
            p50_ns: at(0.50),
            p90_ns: at(0.90),
            p99_ns: at(0.99),
            p999_ns: at(0.999),
            max_ns: samples.last().copied().unwrap_or(0),
        }
    }

    fn json(&self) -> String {
        format!(
            "{{\"n\": {}, \"hits\": {}, \"mean_ns\": {}, \"p50_ns\": {}, \"p90_ns\": {}, \
             \"p99_ns\": {}, \"p999_ns\": {}, \"max_ns\": {}}}",
            self.n,
            self.hits,
            self.mean_ns,
            self.p50_ns,
            self.p90_ns,
            self.p99_ns,
            self.p999_ns,
            self.max_ns,
        )
    }

    fn print(&self, label: &str) {
        println!(
            "{label:<13}: n={} hits={} mean={} ns  p50={} p90={} p99={} p99.9={} max={} ns",
            self.n,
            self.hits,
            self.mean_ns,
            self.p50_ns,
            self.p90_ns,
            self.p99_ns,
            self.p999_ns,
            self.max_ns
        );
    }
}

/// Times `count` operations individually, with `op(k)` reporting whether the
/// result matched the expected answer (returned as `hits`).
fn measure<F>(count: u64, mut op: F) -> Result<Latency, Box<dyn std::error::Error>>
where
    F: FnMut(usize) -> Result<bool, Box<dyn std::error::Error>>,
{
    let mut samples = Vec::with_capacity(count as usize);
    let mut hits = 0u64;
    for k in 0..count as usize {
        let t0 = Instant::now();
        let ok = op(k)?;
        samples.push(t0.elapsed().as_nanos() as u64);
        hits += u64::from(ok);
    }
    Ok(Latency::from_samples(samples, hits))
}

fn write_tsv(path: &Path, rows: &[String]) -> std::io::Result<()> {
    let mut out = BufWriter::new(File::create(path)?);
    for row in rows {
        out.write_all(row.as_bytes())?;
        out.write_all(b"\n")?;
    }
    out.flush()
}

fn run_read(args: &ReadArgs) -> Result<(), Box<dyn std::error::Error>> {
    if args.file.exists() {
        return Err(format!(
            "refusing to overwrite existing file {} (readbench always starts fresh)",
            args.file.display()
        )
        .into());
    }
    let out_dir = args
        .out_dir
        .clone()
        .unwrap_or_else(|| args.file.parent().unwrap_or(Path::new(".")).to_path_buf());
    fs::create_dir_all(&out_dir)?;

    let engine = Engine::create(&args.file, EngineOptions::default())?;
    println!(
        "chronodb readbench: {} records x {} B payload over {} entities, file {}",
        args.records,
        args.payload,
        args.entities,
        args.file.display()
    );

    // ---- write phase: one batch per 4096 records, densely packed pages ----
    let write_start = Instant::now();
    let mut written = 0u64;
    let mut chunk_start = 0u64;
    while chunk_start < args.records {
        let chunk_len = WRITE_BATCH.min((args.records - chunk_start) as usize);
        let payloads: Vec<Vec<u8>> = (0..chunk_len as u64)
            .map(|k| {
                let index = chunk_start + k;
                payload_of(index, (index % args.entities) as u128, args.payload)
            })
            .collect();
        let batch: Vec<(u128, &[u8])> = payloads
            .iter()
            .enumerate()
            .map(|(k, p)| {
                (
                    ((chunk_start + k as u64) % args.entities) as u128,
                    p.as_slice(),
                )
            })
            .collect();
        written += engine.append_batch(&batch)?.len() as u64;
        chunk_start += chunk_len as u64;
    }
    engine.sync()?;
    let write_secs = write_start.elapsed().as_secs_f64();
    if written != args.records || engine.committed_lsn() != args.records {
        return Err(format!(
            "write phase incomplete: appended {written}, committed {}",
            engine.committed_lsn()
        )
        .into());
    }

    // ---- dataset: one strict scan, records in LSN order ----
    // `dataset[i]` is record `i` (LSN `i + 1`): (entity, timestamp).
    let mut dataset: Vec<(u128, u64)> = Vec::with_capacity(args.records as usize);
    {
        let (db, _sb) = DbFile::open_read_only(&args.file)?;
        scan_and_validate(&db, false, |view| {
            dataset.push((view.entity_id(), view.timestamp_ns()));
            Ok(())
        })?;
    }
    if dataset.len() as u64 != args.records {
        return Err(format!(
            "scan found {} records, expected {}",
            dataset.len(),
            args.records
        )
        .into());
    }
    for (i, (entity, _)) in dataset.iter().enumerate() {
        if *entity != (i as u64 % args.entities) as u128 {
            return Err(format!("record {i} landed on the wrong entity").into());
        }
    }
    let file_bytes = fs::metadata(&args.file)?.len();

    // ---- query sets (deterministic from the seed) ----
    let mut rng = Rng::new(args.seed);
    let asof: Vec<(u128, u64)> = (0..args.queries)
        .map(|_| dataset[rng.below(args.records) as usize])
        .collect();
    let latest: Vec<u128> = (0..args.latest)
        .map(|_| rng.below(args.entities) as u128)
        .collect();
    let scans: Vec<u128> = (0..args.scans)
        .map(|_| rng.below(args.entities) as u128)
        .collect();

    write_tsv(
        &out_dir.join("dataset.tsv"),
        &dataset
            .iter()
            .enumerate()
            .map(|(i, (entity, ts))| format!("{i}\t{entity}\t{ts}"))
            .collect::<Vec<_>>(),
    )?;
    write_tsv(
        &out_dir.join("asof.tsv"),
        &asof
            .iter()
            .map(|(entity, ts)| format!("{entity}\t{ts}"))
            .collect::<Vec<_>>(),
    )?;
    write_tsv(
        &out_dir.join("latest.tsv"),
        &latest.iter().map(|e| e.to_string()).collect::<Vec<_>>(),
    )?;
    write_tsv(
        &out_dir.join("scan.tsv"),
        &scans.iter().map(|e| e.to_string()).collect::<Vec<_>>(),
    )?;

    // ---- measure the three read workloads ----
    let asof_stats = measure(args.queries, |k| {
        let (entity, ts) = asof[k];
        let hit = engine.get_as_of(entity, ts)?;
        Ok(hit.is_some_and(|r| {
            let found = r.header.timestamp_ns;
            found == ts
        }))
    })?;
    let latest_stats = measure(args.latest, |k| {
        let entity = latest[k];
        let hit = engine.get_latest(entity)?;
        Ok(hit.is_some_and(|r| {
            let found = r.header.entity_id;
            found == entity
        }))
    })?;
    let scan_stats = measure(args.scans, |k| {
        let entity = scans[k];
        let mut rows = 0u64;
        let mut bytes = 0u64;
        for rec in engine.scan_history(entity, 0, u64::MAX)? {
            let rec = rec?;
            rows += 1;
            bytes += rec.payload().len() as u64;
        }
        Ok(rows > 0 && bytes == rows * args.payload as u64)
    })?;

    // ---- report ----
    let stats = engine.close(true)?;
    println!(
        "write           : {written} records in {:.3} s ({:.0} records/s), \
         {:.1} MiB file, {} data pages",
        write_secs,
        written as f64 / write_secs.max(f64::MIN_POSITIVE),
        file_bytes as f64 / (1u64 << 20) as f64,
        stats.pages_flushed,
    );
    asof_stats.print("as-of");
    latest_stats.print("latest");
    scan_stats.print("entity-scan");

    let json = format!(
        "{{\n  \"engine\": \"chronodb\",\n  \"records\": {},\n  \"entities\": {},\n  \
         \"payload_bytes\": {},\n  \"queries\": {},\n  \"latest_lookups\": {},\n  \
         \"scans\": {},\n  \"seed\": {},\n  \"write_secs\": {:.6},\n  \
         \"file_bytes\": {},\n  \"data_pages\": {},\n  \
         \"asof\": {},\n  \"latest\": {},\n  \"entity_scan\": {}\n}}\n",
        args.records,
        args.entities,
        args.payload,
        args.queries,
        args.latest,
        args.scans,
        args.seed,
        write_secs,
        file_bytes,
        stats.pages_flushed,
        asof_stats.json(),
        latest_stats.json(),
        scan_stats.json(),
    );
    let json_path = args
        .json
        .clone()
        .unwrap_or_else(|| out_dir.join("chronodb.json"));
    fs::write(&json_path, &json)?;

    println!("latency method  : per-call `Instant`, in-process, payload materialised");
    println!("artifact dir    : {}", out_dir.display());
    println!("json            : {}", json_path.display());
    Ok(())
}
