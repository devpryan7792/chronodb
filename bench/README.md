# ChronoDB read-latency benchmark (Phase 5)

Compares ChronoDB's read path against SQLite and DuckDB on the workload the
engine exists for: many small point reads ("what did entity E look like at
time T?"), plus latest-row lookups and per-entity history scans.

## How it works

1. `chronodb readbench` (release binary) creates a fresh database, writes
   `--records` records spread round-robin over `--entities` entities
   (payload size `--payload`, deterministic bytes), then measures three
   workloads with one `Instant` sample per call:
   - **as-of** — `Engine::get_as_of(entity, ts)` for `--queries` random
     (entity, exact-record-timestamp) pairs;
   - **latest** — `Engine::get_latest(entity)` for `--latest` random entities;
   - **entity-scan** — full `scan_history` of `--scans` random entities,
     payloads materialised.
   It verifies every answer (timestamp/entity match, payload byte count) and
   fails on any miss, then writes `dataset.tsv` (`i`, `entity`, `ts`),
   `asof.tsv`, `latest.tsv`, `scan.tsv` and `chronodb.json` to `--out-dir`.
2. `bench/compare_read.py` ingests `dataset.tsv` into SQLite (stdlib
   `sqlite3`) and DuckDB (if importable), regenerates the identical payload
   bytes, and replays the same query files with the same per-call timing.
   Record payloads are regenerated with the same formula as the Rust side:
   `byte k of record i = (i * 31 + entity + k) & 0xFF`.

The SQL equivalents are the standard as-of form:

```sql
-- as-of
SELECT payload FROM events WHERE entity = ? AND ts <= ? ORDER BY ts DESC LIMIT 1
-- latest
SELECT payload FROM events WHERE entity = ? ORDER BY ts DESC LIMIT 1
-- entity scan
SELECT payload FROM events WHERE entity = ? ORDER BY ts
```

with `CREATE INDEX idx_events ON events (entity, ts)` in both engines.

## Reproducing

```sh
cargo build --release
python3 bench/compare_read.py                       # SQLite only
python3 -m venv /tmp/opencode/duckvenv              # optional: DuckDB in an
/tmp/opencode/duckvenv/bin/pip install duckdb       # isolated venv
python3 bench/compare_read.py --duckdb-python /tmp/opencode/duckvenv/bin/python
```

Useful knobs: `--records`, `--entities`, `--payload`, `--queries`, `--latest`,
`--scans`, `--seed`, `--workdir`, `--json`. All artifacts (both databases, the
combined `comparison.json`) land in `--workdir`.

## Results

Recorded runs live in [`bench/results/`](results/) as JSON; the newest one is
rendered into the README chart by `python3 bench/render_charts.py` (stdlib
only), so the picture can never drift from the numbers.

Release build, Arch Linux (Python 3.14, SQLite 3.x, DuckDB 1.5.6 in a venv),
200 000 records × 64 B payload over 1 000 entities on tmpfs, 50 000 as-of
queries / 20 000 latest lookups / 2 000 entity scans. Every engine resolved
100 % of the queries. Lower is better; "rel" is the p50 ratio vs ChronoDB.

| workload    | engine      | mean   | p50    | p90    | p99    | p99.9  | max    | rel p50    |
|-------------|-------------|--------|--------|--------|--------|--------|--------|------------|
| as-of (µs)  | chronodb    | 0.60   | 0.50   | 0.85   | 2.08   | 11.05  | 27.95  | 1.00x      |
|             | sqlite      | 6.95   | 6.28   | 7.75   | 18.78  | 75.40  | 308.92 | 12.50x     |
|             | sqlite-mmap | 5.83   | 5.16   | 6.81   | 15.44  | 54.66  | 368.30 | 10.27x     |
|             | duckdb      | 1445.4 | 1366.2 | 1780.0 | 2502.9 | 4048.4 | 7773.9 | 2721.6x    |
| latest (µs) | chronodb    | 0.18   | 0.16   | 0.21   | 0.41   | 0.82   | 13.60  | 1.00x      |
|             | sqlite      | 5.52   | 5.11   | 5.83   | 11.95  | 40.31  | 369.76 | 31.96x     |
|             | sqlite-mmap | 5.34   | 4.61   | 7.11   | 12.92  | 57.79  | 376.66 | 28.83x     |
|             | duckdb      | 1200.0 | 1143.2 | 1430.1 | 1945.8 | 3082.3 | 4568.4 | 7145.1x    |
| scan (µs)   | chronodb    | 23.72  | 23.64  | 28.71  | 45.21  | 53.54  | 58.90  | 1.00x      |
|             | sqlite      | 242.30 | 241.12 | 291.00 | 375.71 | 468.52 | 484.83 | 10.20x     |
|             | sqlite-mmap | 135.17 | 131.48 | 164.92 | 232.26 | 282.21 | 539.07 | 5.56x      |
|             | duckdb      | 1328.4 | 1194.6 | 1637.1 | 2776.4 | 5323.5 | 7060.9 | 50.53x     |

Load/build time and storage for the same dataset:

| engine      | ingest + index | file size | settings                        |
|-------------|----------------|-----------|---------------------------------|
| chronodb    | 0.052 s        | 21.2 MiB  | mmap + sparse index, in-process |
| sqlite      | 1.218 s        | 20.4 MiB  | stdlib defaults                 |
| sqlite-mmap | (reuses above) | 20.4 MiB  | `mmap_size=256 MiB`             |
| duckdb      | 107.9 s        | 13.0 MiB  | ART index on `(entity, ts)`     |

## Caveats

- **Measurement method is not identical.** ChronoDB is measured in-process in
  Rust; SQLite and DuckDB are driven from Python, so their numbers include the
  Python client-call overhead (statement bind/execute/fetch). That overhead is
  conservative for them — it inflates their times — but of the same order as
  SQLite's own query cost, so SQLite's "true" cost is likely closer to half the
  reported figures. ChronoDB's per-call `Instant` overhead (~20 ns) is visible
  at the p50 but negligible against its ~500 ns query cost.
- **`sqlite-mmap` mirrors ChronoDB's memory-mapped access path** (the
  `PRAGMA mmap_size` row); the `sqlite` row is stock defaults (page cache +
  `pread`).
- **DuckDB is an OLAP engine.** `EXPLAIN ANALYZE` shows a sequential scan for
  this workload (the ART index is only consulted for covered index lookups),
  so its rows reflect full-table work per query — included for the record, not
  as a point-read baseline. Its build time is dominated by Python-side
  `executemany` row-at-a-time inserts, not the engine.
- **tmpfs, warm cache, single thread.** Everything runs from the OS page cache
  on tmpfs; these are latency-structure comparisons, not cold-storage or
  concurrency benchmarks. ChronoDB's as-of path additionally CRC-verifies the
  record it returns on every read.
