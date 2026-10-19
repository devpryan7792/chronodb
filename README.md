# ChronoDB

[![CI](https://github.com/devpryan7792/chronodb/actions/workflows/ci.yml/badge.svg)](https://github.com/devpryan7792/chronodb/actions/workflows/ci.yml)
[![License: MIT OR Apache-2.0](https://img.shields.io/badge/license-MIT%20OR%20Apache--2.0-blue.svg)](#license)
[![Rust 1.80+](https://img.shields.io/badge/rust-1.80%2B-orange.svg)](#quick-start)

An embedded, append-only **time-travel storage engine** in Rust.

Every write is a versioned, timestamped record. Reads resolve *"what did entity
`E` look like at time `T`?"* directly out of a memory-mapped file — zero-copy,
CRC-verified, no background compaction, no write-ahead log to replay.

```rust
let engine = Engine::create("events.cdb".as_ref(), EngineOptions::default())?;

engine.append_at(42, 1_700_000_000_000_000_000, b"temperature=21.4")?;
engine.append_at(42, 1_700_000_001_000_000_000, b"temperature=19.8")?;

let past = engine.get_as_of(42, 1_700_000_000_500_000_000)?.unwrap();
assert_eq!(past.payload(), b"temperature=21.4");
```

## Highlights

- **Time travel** — `get_as_of(entity, t)`, `get_latest(entity)` and
  `scan_history(entity, from, to)` over one append-only file.
- **Zero-copy reads** — payloads are borrowed slices of an `mmap`; records that
  span page boundaries are reassembled into an owned buffer (`Cow`).
- **Crash safe** — sealed pages are `fdatasync`ed before their appends are
  acknowledged; a torn or corrupt tail is cut back to the last valid record at
  boot, and killed-mid-write processes never lose an acknowledged record
  (verified by a `SIGKILL` + randomized bit-flip fuzzer).
- **Verified data** — CRC32-C over every page and every record, with SSE 4.2
  runtime dispatch and a slicing-by-8 software fallback.
- **Single writer, many readers** — producers share a lock-free SPSC ring, one
  writer thread packs 4 KiB pages, readers never take a lock on the data path.
- **Small dependency surface** — `byteorder`, `thiserror`, `parking_lot`,
  `libc` (Unix only).
- **Tooling included** — `chronoctl` for inspection/verification/repair and two
  benchmark harnesses, one of which measures read latency against SQLite and
  DuckDB.

## Quick start

```sh
cargo build --release
cargo run --example time_travel      # append, time-travel, close, reopen
```

Library use (the full runnable version is [`examples/time_travel.rs`](examples/time_travel.rs)):

```rust
use chronodb::engine::{Engine, EngineOptions};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let engine = Engine::create("events.cdb".as_ref(), EngineOptions::default())?;

    engine.append_at(42, 1_700_000_000_000_000_000, b"temperature=21.4")?;
    engine.append_at(42, 1_700_000_001_000_000_000, b"temperature=19.8")?;

    let past = engine.get_as_of(42, 1_700_000_000_500_000_000)?.unwrap();
    assert_eq!(past.payload(), b"temperature=21.4");

    for rec in engine.scan_history(42, 0, u64::MAX)? {
        println!("{} @ {}", String::from_utf8_lossy(rec?.payload()), rec?.timestamp_ns());
    }

    engine.sync()?;              // durability barrier
    engine.close(true)?;         // clean shutdown, final sync
    Ok(())
}
```

Appends are acknowledged by policy: `Durability::PerBatch` (default) acks after
`fdatasync`, `Durability::OnSync` acks once the page is written. `append` stamps
the record from the wall clock (clamped monotonic), `append_at` uses an explicit
timestamp that must exceed every timestamp committed so far.

## How it works

```text
        append / append_at / append_batch
                     │
                     ▼
        ┌────────────────────────┐        ┌──────────────────────────┐
        │  SPSC ring (1 MiB)     │───────►│  writer thread           │
        │  + ack slab            │        │  packs 4 KiB pages,      │
        └────────────────────────┘        │  seals, writes, fdatasync│
                     ▲                    └────────────┬─────────────┘
                     │ acks                            │  pwrite
                     │                                 ▼
        ┌────────────┴───────────┐        ┌──────────────────────────┐
        │  sparse radix index    │◄───────│  database file (mmap)    │
        │  256 sharded ART nodes │        │  superblock + pages      │
        └────────────────────────┘        └────────────┬─────────────┘
                     ▲                                 │ zero-copy slices
                     └──── get_as_of / get_latest ─────┘   / scan_history
```

**On-disk format** (little-endian throughout):

| Structure      | Size      | Contents                                                            |
|----------------|-----------|---------------------------------------------------------------------|
| Superblock     | 4096 B    | page 0: magic, format version, counters, CRC32-C                    |
| Page header    | 32 B      | epoch (= page index), page LSN range, flags, CRC over the page      |
| Record header  | 44 B      | LSN, entity id (`u128`), timestamp (ns), payload length, flags, CRC |
| Payload        | variable  | may span contiguous pages; sealed pages are zero-padded             |

The engine is split into three layers: `storage` (format, CRC, file I/O, page
packer, recovery scanner), `index` (`mmap` regions + the sharded radix index)
and `engine` (SPSC ring, writer thread, acks, read path, `Engine` facade). The
design document with the full format tables and rationale is
[`SPECIFICATION.md`](SPECIFICATION.md).

**Recovery contract** — on open the tail is validated page by page (CRC, page
epochs, LSN/timestamp continuity). A torn or corrupt tail is cut back to the
first page of the damaged record, the sparse index is rebuilt from the surviving
records and the writer resumes with the recovered LSN, clock and page cursors.
Two fuzzed guarantees: every acknowledged record survives (*completeness*), and
recovery never invents a record — the recovered set is always a contiguous,
byte-exact prefix of what was written (*soundness*).

## Performance

Read latency on 200 000 records × 64 B over 1 000 entities (release build,
tmpfs, warm cache; all 50 000 as-of queries resolved). Full methodology,
artifacts and caveats in [`bench/README.md`](bench/README.md):

| workload              | chronodb     | SQLite       | SQLite (mmap) | DuckDB        |
|-----------------------|--------------|--------------|---------------|---------------|
| as-of p50 / p99       | **0.50 / 2.08 µs** | 6.28 / 18.78 µs | 5.16 / 15.44 µs | 1.37 / 2.50 ms |
| latest p50            | **0.16 µs**  | 5.11 µs      | 4.61 µs       | 1.14 ms       |
| entity scan p50 (200 records) | **23.6 µs** | 241 µs | 131 µs   | 1.19 ms       |
| ingest + index 200k records | **0.052 s** | 1.22 s | —        | 107.9 s       |

Reproduce with:

```sh
cargo build --release
python3 bench/compare_read.py            # SQLite via the stdlib
# DuckDB (optional, isolated venv):
python3 -m venv /tmp/duckvenv && /tmp/duckvenv/bin/pip install duckdb
python3 bench/compare_read.py --duckdb-python /tmp/duckvenv/bin/python
```

The write path has its own harness:

```sh
cargo run --release --bin chronodb -- bench --records 500000 --payload 128 --threads 4
```

## CLI

```text
chronodb readbench [--records N] [--entities M] [--payload B] [--queries Q]
                   [--latest L] [--scans S] [--seed S] [--out-dir DIR] [--json PATH]
chronodb bench     [--file PATH] [--records N] [--payload BYTES] [--threads T]

chronoctl inspect <file>                 # superblock + page/record summary
chronoctl verify  <file> [--repair]      # full CRC walk, optional tail repair
chronoctl recover <file>                 # SPEC §5.2 boot path, prints what it cut
chronoctl tail    <file> [count]         # last N records
chronoctl query   <file> --id <uuid|hex|dec> [--as-of <rfc3339|unix-ns>]
```

## Testing

```sh
cargo test                                       # debug
cargo test --release                             # release
cargo clippy --all-targets -- -D warnings
cargo fmt --check
```

The suite covers each layer and the failure modes that matter:

| Suite                    | What it pins down                                                        |
|--------------------------|--------------------------------------------------------------------------|
| `tests/storage_page.rs`  | page/record layout, CRC32-C vectors, sealing and span packing            |
| `tests/write_path.rs`    | ring + writer semantics, both durability policies, batching, concurrency |
| `tests/read_path.rs`     | as-of/latest/history resolution, tombstone filtering, mmap growth         |
| `tests/recovery.rs`      | torn tails, bit flips, partial pages, tombstone and cursor resume        |
| `tests/crash_recovery.rs`| `SIGKILL` completeness + 120-case randomized tail-damage fuzzer           |
| `tests/cli_query.rs`, `tests/cli_recover.rs` | `chronoctl` end to end                             |

## Project layout

```text
src/
  storage/     on-disk format (page.rs, record.rs), CRC32-C, pwrite layer,
               page packer/writer, recovery + validation scanner
  index/       memory-mapped regions (mmap.rs), sharded adaptive radix index
  engine/      SPSC ring, writer thread, ack slab, read path, Engine facade
  bin/         chronodb (benchmarks) and chronoctl (inspection/repair) CLIs
tests/         layer suites, CLIs, crash-recovery harness
bench/         read-latency comparison harness (SQLite/DuckDB) + results
examples/      time_travel.rs
SPECIFICATION.md   design document: format tables, contracts, roadmap
```

## Status

All five planned phases are implemented and verified — on-disk format and
low-level I/O, append-only write path with the SPSC ring, memory-mapped read
path and index, crash recovery and corruption repair, and the adversarial
fuzzing + benchmark phase. The roadmap in [`SPECIFICATION.md`](SPECIFICATION.md)
tracks the details, including the handful of documented deviations from the
original sketches.

## License

Licensed under either of

- Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE))
- MIT license ([LICENSE-MIT](LICENSE-MIT))

at your option.

### Contribution

Unless you explicitly state otherwise, any contribution intentionally submitted
for inclusion in the work by you, as defined in the Apache-2.0 license, shall be
dual licensed as above, without any additional terms or conditions.
