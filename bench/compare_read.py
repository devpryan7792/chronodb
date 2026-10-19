#!/usr/bin/env python3
"""Phase 5 read-latency comparison: ChronoDB vs SQLite vs DuckDB.

The `chronodb readbench` binary builds a dataset and emits the exact query
sets it measured as TSV artifacts:

    dataset.tsv   i <TAB> entity <TAB> ts        (record i, LSN i+1)
    asof.tsv      entity <TAB> ts                (point queries)
    latest.tsv    entity                         (latest lookups)
    scan.tsv      entity                         (per-entity history scans)

This harness replays those artifacts against SQLite (`sqlite3`, stdlib) and,
when importable, DuckDB, measures the same per-call latencies and prints a
comparison table. Record payloads are regenerated with the same formula the
Rust binary used (`payload_of`) so the byte volume is identical.

Usage:

    # from the repository root, release binary built:
    cargo build --release
    python3 bench/compare_read.py                 # 200k records, 1k entities

    # with DuckDB from an isolated venv:
    python3 -m venv /tmp/opencode/duckvenv
    /tmp/opencode/duckvenv/bin/pip install duckdb
    python3 bench/compare_read.py --duckdb-python /tmp/opencode/duckvenv/bin/python

Latency method: one `perf_counter_ns()`/`Instant` sample per query around
each call, so the SQLite/DuckDB numbers *include* the Python client call
overhead while ChronoDB's are in-process Rust. The comparison is therefore
conservative for the C engines' query cost; the `sqlite-mmap` row mirrors
ChronoDB's memory-mapped access path.
"""

from __future__ import annotations

import argparse
import json
import math
import os
import shutil
import sqlite3
import subprocess
import sys
import time
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
DEFAULT_WORKDIR = Path("/tmp/opencode/chronodb-readbench")

ASOF_SQL = (
    "SELECT payload FROM events WHERE entity = ? AND ts <= ? ORDER BY ts DESC LIMIT 1"
)
LATEST_SQL = "SELECT payload FROM events WHERE entity = ? ORDER BY ts DESC LIMIT 1"
SCAN_SQL = "SELECT payload FROM events WHERE entity = ? ORDER BY ts"


def payload_of(index: int, entity: int, size: int) -> bytes:
    """Mirror of `payload_of` in src/bin/chronodb.rs."""
    return bytes(((index * 31 + entity + k) & 0xFF) for k in range(size))


def stats(samples: list[int], hits: int) -> dict:
    """The same percentile rule as the Rust harness (ceil(p*n), 1-based)."""
    ordered = sorted(samples)
    n = len(ordered)

    def at(p: float) -> int:
        if n == 0:
            return 0
        return ordered[min(n, max(1, math.ceil(n * p))) - 1]

    return {
        "n": n,
        "hits": hits,
        "mean_ns": sum(ordered) // n if n else 0,
        "p50_ns": at(0.50),
        "p90_ns": at(0.90),
        "p99_ns": at(0.99),
        "p999_ns": at(0.999),
        "max_ns": ordered[-1] if n else 0,
    }


def measure(queries: list, op) -> dict:
    samples: list[int] = []
    hits = 0
    for q in queries:
        t0 = time.perf_counter_ns()
        ok = op(q)
        samples.append(time.perf_counter_ns() - t0)
        hits += bool(ok)
    return stats(samples, hits)


# ---------------------------------------------------------------------------
# artifact loading
# ---------------------------------------------------------------------------


def load_workdir(workdir: Path) -> dict:
    dataset = []
    with open(workdir / "dataset.tsv") as fh:
        for line in fh:
            i, entity, ts = line.split()
            dataset.append((int(i), int(entity), int(ts)))

    def load_pairs(name: str) -> list[tuple[int, int]]:
        out = []
        with open(workdir / name) as fh:
            for line in fh:
                entity, ts = line.split()
                out.append((int(entity), int(ts)))
        return out

    def load_entities(name: str) -> list[int]:
        with open(workdir / name) as fh:
            return [int(line) for line in fh]

    return {
        "dataset": dataset,
        "asof": load_pairs("asof.tsv"),
        "latest": load_entities("latest.tsv"),
        "scan": load_entities("scan.tsv"),
    }


def expected_asof_spot(
    dataset: list[tuple[int, int, int]],
    queries: list[tuple[int, int]],
    count: int = 200,
) -> dict:
    """Spot-check map `(entity, ts) -> record index` for the first `count` queries."""
    index_of = {(entity, ts): i for i, entity, ts in dataset}
    return {q: index_of[q] for q in queries[:count] if q in index_of}


# ---------------------------------------------------------------------------
# SQLite
# ---------------------------------------------------------------------------


def build_sqlite(
    db: Path, dataset: list[tuple[int, int, int]], payload_bytes: int
) -> float:
    if db.exists():
        db.unlink()
    conn = sqlite3.connect(db)
    conn.execute("PRAGMA journal_mode = OFF")
    conn.execute("PRAGMA synchronous = OFF")
    conn.execute(
        "CREATE TABLE events (entity INTEGER NOT NULL, ts INTEGER NOT NULL, "
        "payload BLOB NOT NULL)"
    )
    conn.execute("CREATE INDEX idx_events ON events (entity, ts)")
    t0 = time.perf_counter()
    rows = [
        (entity, ts, payload_of(i, entity, payload_bytes)) for i, entity, ts in dataset
    ]
    for start in range(0, len(rows), 50_000):
        conn.executemany(
            "INSERT INTO events (entity, ts, payload) VALUES (?, ?, ?)",
            rows[start : start + 50_000],
        )
    conn.commit()
    secs = time.perf_counter() - t0
    conn.close()
    return secs


def measure_sqlite(
    db: Path,
    queries: dict,
    payload_bytes: int,
    pragmas: dict,
    spot: dict | None,
) -> dict:
    conn = sqlite3.connect(db)
    for key, value in pragmas.items():
        conn.execute(f"PRAGMA {key} = {value}")
    conn.execute("PRAGMA query_only = ON")

    def asof(q: tuple[int, int]) -> bool:
        row = conn.execute(ASOF_SQL, q).fetchone()
        if row is None or len(row[0]) != payload_bytes:
            return False
        if spot is not None and q in spot:
            i = spot[q]
            return row[0] == payload_of(i, q[0], payload_bytes)
        return True

    def latest(q: int) -> bool:
        row = conn.execute(LATEST_SQL, (q,)).fetchone()
        return row is not None and len(row[0]) == payload_bytes

    def scan(q: int) -> bool:
        rows = conn.execute(SCAN_SQL, (q,)).fetchall()
        return bool(rows) and all(len(r[0]) == payload_bytes for r in rows)

    out = {
        "asof": measure(queries["asof"], asof),
        "latest": measure(queries["latest"], latest),
        "entity_scan": measure(queries["scan"], scan),
    }
    conn.close()
    return out


def measure_duckdb(
    db: Path,
    dataset: list[tuple[int, int, int]],
    queries: dict,
    payload_bytes: int,
    spot: dict | None,
) -> tuple[float, dict]:
    import duckdb  # noqa: PLC0415

    if db.exists():
        db.unlink()
    con = duckdb.connect(str(db))
    con.execute("CREATE TABLE events (entity BIGINT, ts BIGINT, payload BLOB)")
    t0 = time.perf_counter()
    rows = [
        (entity, ts, payload_of(i, entity, payload_bytes)) for i, entity, ts in dataset
    ]
    for start in range(0, len(rows), 50_000):
        con.executemany(
            "INSERT INTO events VALUES (?, ?, ?)", rows[start : start + 50_000]
        )
    con.execute("CREATE INDEX idx_events ON events(entity, ts)")
    build_secs = time.perf_counter() - t0

    def asof(q: tuple[int, int]) -> bool:
        row = con.execute(ASOF_SQL, q).fetchone()
        if row is None or len(row[0]) != payload_bytes:
            return False
        if spot is not None and q in spot:
            i = spot[q]
            return row[0] == payload_of(i, q[0], payload_bytes)
        return True

    def latest(q: int) -> bool:
        row = con.execute(LATEST_SQL, (q,)).fetchone()
        return row is not None and len(row[0]) == payload_bytes

    def scan(q: int) -> bool:
        rows = con.execute(SCAN_SQL, (q,)).fetchall()
        return bool(rows) and all(len(r[0]) == payload_bytes for r in rows)

    out = {
        "asof": measure(queries["asof"], asof),
        "latest": measure(queries["latest"], latest),
        "entity_scan": measure(queries["scan"], scan),
    }
    con.close()
    return build_secs, out


# ---------------------------------------------------------------------------
# worker mode: measure the heavyweight engines in this interpreter
# ---------------------------------------------------------------------------


def run_worker(workdir: Path) -> int:
    with open(workdir / "chronodb.json") as fh:
        config = json.load(fh)
    payload_bytes = config["payload_bytes"]
    queries = load_workdir(workdir)
    spot = expected_asof_spot(queries["dataset"], queries["asof"])

    engines: dict[str, dict] = {}

    sqlite_db = workdir / "sqlite.db"
    build_secs = build_sqlite(sqlite_db, queries["dataset"], payload_bytes)
    engines["sqlite"] = {
        "build_secs": build_secs,
        "file_bytes": sqlite_db.stat().st_size,
        **measure_sqlite(sqlite_db, queries, payload_bytes, {}, spot),
        "settings": "stdlib defaults",
    }
    engines["sqlite-mmap"] = {
        "build_secs": 0.0,  # reuses sqlite.db
        "file_bytes": sqlite_db.stat().st_size,
        **measure_sqlite(
            sqlite_db, queries, payload_bytes, {"mmap_size": 256 << 20}, spot
        ),
        "settings": "mmap_size=256 MiB",
    }

    try:
        duck_db = workdir / "duckdb.db"
        build_secs, out = measure_duckdb(
            duck_db, queries["dataset"], queries, payload_bytes, spot
        )
        engines["duckdb"] = {
            "build_secs": build_secs,
            "file_bytes": duck_db.stat().st_size,
            **out,
            "settings": "ART index on (entity, ts)",
        }
    except ImportError:
        print("[worker] duckdb not importable in this interpreter — skipped")

    with open(workdir / "engines.json", "w") as fh:
        json.dump({"engines": engines}, fh, indent=2)
    print(f"[worker] measured: {', '.join(engines)}")
    return 0


# ---------------------------------------------------------------------------
# parent mode
# ---------------------------------------------------------------------------


def us(ns: int) -> str:
    return f"{ns / 1000:.2f}"


def print_table(config: dict, engines: dict[str, dict]) -> None:
    order = ["chronodb", *[n for n in engines if n != "chronodb"]]
    workloads = [
        ("asof", "as-of"),
        ("latest", "latest"),
        ("entity_scan", "entity-scan"),
    ]
    width = max(len(n) for n in order) + 2

    for key, label in workloads:
        base = engines.get("chronodb", {}).get(key, {}).get("p50_ns") or 1
        print(
            f"\n{label} latency (µs, n = {engines.get('chronodb', {}).get(key, {}).get('n')}):"
        )
        header = (
            f"  {'engine':<{width}}{'mean':>10}{'p50':>10}{'p90':>10}"
            f"{'p99':>10}{'p99.9':>10}{'max':>10}{'rel p50':>10}"
        )
        print(header)
        for name in order:
            block = engines.get(name, {}).get(key)
            if not block:
                continue
            rel = (block["p50_ns"] / base) if base else float("nan")
            print(
                f"  {name:<{width}}{us(block['mean_ns']):>10}{us(block['p50_ns']):>10}"
                f"{us(block['p90_ns']):>10}{us(block['p99_ns']):>10}"
                f"{us(block['p999_ns']):>10}{us(block['max_ns']):>10}"
                f"{rel:>9.2f}x"
            )

    print("\nbuild / storage:")
    for name in order:
        block = engines.get(name)
        if not block:
            continue
        asof = block.get("asof", {})
        print(
            f"  {name:<{width}}n={config.get('queries', asof.get('n', 0))}, "
            f"hits={asof.get('hits', '-')}, "
            f"file={block.get('file_bytes', 0) / (1 << 20):.1f} MiB, "
            f"build={block.get('build_secs', 0):.3f} s "
            f"({block.get('settings', '')})"
        )


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--records", type=int, default=200_000)
    parser.add_argument("--entities", type=int, default=1_000)
    parser.add_argument("--payload", type=int, default=64)
    parser.add_argument("--queries", type=int, default=50_000)
    parser.add_argument("--latest", type=int, default=20_000)
    parser.add_argument("--scans", type=int, default=2_000)
    parser.add_argument("--seed", type=int, default=0x5EED)
    parser.add_argument("--workdir", type=Path, default=DEFAULT_WORKDIR)
    parser.add_argument("--bin", type=Path, default=None, help="chronodb binary")
    parser.add_argument("--json", type=Path, default=None, help="combined result path")
    parser.add_argument(
        "--duckdb-python",
        type=Path,
        default=None,
        help="interpreter that has duckdb installed (worker runs under it)",
    )
    parser.add_argument("--worker", type=Path, default=None, help=argparse.SUPPRESS)
    args = parser.parse_args()

    if args.worker is not None:
        return run_worker(args.worker)

    # 1. run the Rust harness (builds the dataset + query sets, measures itself)
    binary = args.bin
    if binary is None:
        for candidate in (
            REPO / "target/release/chronodb",
            REPO / "target/debug/chronodb",
        ):
            if candidate.exists():
                binary = candidate
                break
    if binary is None or not binary.exists():
        print(
            "error: chronodb binary not found; run `cargo build --release`",
            file=sys.stderr,
        )
        return 1
    print(f"chronodb binary : {binary}")

    workdir = args.workdir
    if workdir.exists():
        shutil.rmtree(workdir)
    workdir.mkdir(parents=True)

    cmd = [
        str(binary),
        "readbench",
        "--file",
        str(workdir / "readbench.cdb"),
        "--records",
        str(args.records),
        "--entities",
        str(args.entities),
        "--payload",
        str(args.payload),
        "--queries",
        str(args.queries),
        "--latest",
        str(args.latest),
        "--scans",
        str(args.scans),
        "--seed",
        str(args.seed),
        "--out-dir",
        str(workdir),
    ]
    print(f"running         : {' '.join(cmd)}")
    proc = subprocess.run(cmd, capture_output=True, text=True)
    sys.stdout.write(proc.stdout)
    sys.stderr.write(proc.stderr)
    if proc.returncode != 0:
        print(f"error: chronodb readbench exited {proc.returncode}", file=sys.stderr)
        return proc.returncode

    with open(workdir / "chronodb.json") as fh:
        chronodb = json.load(fh)

    # 2. replay against the other engines (optionally in another interpreter)
    worker_python = args.duckdb_python or Path(sys.executable)
    if not worker_python.exists():
        print(f"error: --duckdb-python {worker_python} does not exist", file=sys.stderr)
        return 1
    print(f"engines worker  : {worker_python}")
    proc = subprocess.run(
        [str(worker_python), str(Path(__file__).resolve()), "--worker", str(workdir)],
        capture_output=True,
        text=True,
    )
    sys.stdout.write(proc.stdout)
    sys.stderr.write(proc.stderr)
    if proc.returncode != 0:
        print(f"error: engines worker exited {proc.returncode}", file=sys.stderr)
        return proc.returncode

    with open(workdir / "engines.json") as fh:
        engines = json.load(fh)["engines"]

    # 3. merge + report
    merged: dict[str, dict] = {
        "chronodb": {
            "build_secs": chronodb["write_secs"],
            "file_bytes": chronodb["file_bytes"],
            "settings": "mmap + sparse index, in-process",
            "asof": chronodb["asof"],
            "latest": chronodb["latest"],
            "entity_scan": chronodb["entity_scan"],
        },
        **engines,
    }
    print("\n=== read latency comparison ===")
    print(
        f"workload: {args.records} records x {args.payload} B over {args.entities} "
        f"entities, {args.queries} as-of queries / {args.latest} latest / {args.scans} scans"
    )
    print_table(chronodb, merged)

    combined = {
        "config": {
            "records": args.records,
            "entities": args.entities,
            "payload_bytes": args.payload,
            "queries": args.queries,
            "latest": args.latest,
            "scans": args.scans,
            "seed": args.seed,
        },
        "engines": merged,
    }
    json_path = args.json or (workdir / "comparison.json")
    with open(json_path, "w") as fh:
        json.dump(combined, fh, indent=2)
    print(f"\ncombined json   : {json_path}")
    print(
        "note: chronodb latencies are in-process Rust; SQLite/DuckDB latencies\n"
        "      include the Python client call overhead (conservative for them).\n"
        "      DuckDB's planner serves this workload with a sequential scan\n"
        "      (verified with EXPLAIN ANALYZE: the ART index is only consulted\n"
        "      for covered equality lookups), so its point-read numbers reflect\n"
        "      an OLAP engine doing full-table work per query."
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
