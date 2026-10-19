# ChronoDB: Embedded Append-Only Time-Travel Storage Engine
## Engineering Specification Document (v1.0.0-PROD)
**Author:** Antigravity Storage & Systems Architecture Group  
**Target Architecture:** Linux (x86_64, aarch64), POSIX-compliant kernels  
**Primary Language Target:** Rust (MSRV 1.80+) or Zig (0.13+) or C++20  
**Storage Model:** Append-Only Write-Ahead Log (WAL) + Memory-Mapped Sparse Primary Index (mmap) + AS-OF MVCC Engine  
**Status:** Approved for Implementation  

---

## 1. Executive Summary & Problem Formulation

### 1.1 The Problem
Traditional relational databases (PostgreSQL, MySQL) and key-value stores (RocksDB, LevelDB) are optimized for in-place mutation or log-structured merge compaction where older versions of records are eventually discarded or garbage collected. When applications require strict point-in-time time-travel (e.g., financial audit trails, state-machine deterministic replays, AI agent execution history, or compliance logs):
1. **Compaction Destroys History:** Standard LSM-trees tombstone and compact away intermediate states.
2. **Slow Historical Queries:** Simulating time-travel via `valid_from` / `valid_to` SQL tables requires bloated B-Tree indices and expensive range scans with table locks.
3. **Heavy Footprint:** Systems like DuckDB or ClickHouse are analytical powerhouses but carry large dependencies, lack embeddable sub-millisecond point-write guarantees, and do not provide zero-copy memory-mapped point lookups.

### 1.2 The Solution: ChronoDB
`ChronoDB` is an embeddable, single-file, zero-dependency, crash-resilient storage engine designed exclusively for append-only, immutable event logs and high-throughput time-travel queries. 

Key Architectural Highlights:
* **Zero Mutation:** Data pages are write-once, append-only.
* **Direct I/O & Memory Mapping:** Writes bypass kernel page cache via `O_DIRECT` WAL append; reads utilize memory-mapped (`mmap`) sparse index segments for sub-microsecond `AS OF` point queries.
* **Torn-Write Resilience:** Every 4096-byte page boundary is guarded by CRC32-C checksums and monotonic epoch sequence numbers inspired by TigerBeetle.
* **Zero-Copy Traversal:** Query results point directly to memory-mapped slices without buffer allocations.

```
                           +------------------------+
                           |    Client API / SDK    |
                           +-----------+------------+
                                       |
                   +-------------------+-------------------+
                   | (Write Path: Append)                  | (Read Path: AS OF)
                   v                                       v
        +----------------------+               +-----------------------+
        |  Lock-Free Ring      |               |  Time-Travel Query    |
        |  Buffer (Single W)   |               |  Engine (MVCC)        |
        +----------+-----------+               +-----------+-----------+
                   |                                       |
                   v                                       |
        +----------------------+                           |
        |  Direct I/O Appender |                           |
        | (4KB-aligned pages)  |                           |
        +----------+-----------+                           |
                   |                                       |
                   v (fsync / monotonic)                   v (Binary Search)
    +--------------------------------------------------------------+
    |                     chronodb_data.cdb                        |
    |  +--------------------+----------------+-------------------+ |
    |  | File Header (64 B) | Data Segments  | Sparse Index Page | |
    |  | (Magic, Version)   | (Immutable)    | (Timestamp / Pos) | |
    |  +--------------------+----------------+-------------------+ |
    +--------------------------------------------------------------+
```

---

## 2. On-Disk Binary Format Specification

A ChronoDB database consists of a single contiguous file (`*.cdb`) composed of 4096-byte (4 KiB) aligned blocks.

### 2.1 File Layout Overview
```
+-------------------------------------------------------------------------+
| Superblock (Page 0: Bytes 0 - 4095)                                     |
+-------------------------------------------------------------------------+
| Segment 0: WAL Record Pages (Pages 1 ... N)                             |
+-------------------------------------------------------------------------+
| Index Chunk 0: Sparse Time-Key Index (Pages N+1 ... M)                  |
+-------------------------------------------------------------------------+
| ... Segment K + Index Chunk K ...                                       |
+-------------------------------------------------------------------------+
| Trailer Block (Last 4096 bytes: Commit Checkpoint & Root Pointer)       |
+-------------------------------------------------------------------------+
```

### 2.2 Superblock Structure (Page 0)
Offset: `0x00000000`, Size: 4096 bytes (Little-Endian).

| Offset | Field | Type | Size | Description |
| :--- | :--- | :--- | :--- | :--- |
| `0x00` | `magic` | `[u8; 8]` | 8 B | ASCII `"CHRONODB"` (`0x43 0x48 0x52 0x4F 0x4E 0x4F 0x44 0x42`) |
| `0x08` | `version` | `u32` | 4 B | File format version (current: `1`) |
| `0x0C` | `page_size` | `u32` | 4 B | Page size in bytes (fixed: `4096`) |
| `0x10` | `created_at` | `u64` | 8 B | Unix epoch nanoseconds of creation |
| `0x18` | `cluster_id` | `u128`| 16 B | Unique 128-bit DB identifier |
| `0x28` | `flags` | `u32` | 4 B | Bit 0: Strict Direct I/O; Bit 1: Compressed payloads |
| `0x2C` | `reserved` | `[u8; 4048]` | 4048 B | Zero-padded for future extensions |
| `0xFFC`| `checksum` | `u32` | 4 B | CRC32-C of bytes `0x00..0xFFB` |

### 2.3 Record Page Header (Every 4 KiB Block)
Every record data page starts with a 32-byte header:

```
 0                   1                   2                   3
 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|                          Page Epoch                           |
|                           (64-bit)                            |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|                        Previous Page LSN                      |
|                           (64-bit)                            |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|         Record Count          |           Flags               |
|           (16-bit)            |          (16-bit)             |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|                      CRC32-C Page Checksum                    |
|                           (32-bit)                            |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|                           Reserved                            |
|                           (32-bit)                            |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
```

### 2.4 Event Record Binary Layout
Within a page, events are serialized contiguously:

```rust
#[repr(C, packed)]
pub struct RecordHeader {
    pub lsn: u64,             // Log Sequence Number (globally monotonic)
    pub timestamp_ns: u64,    // Unix epoch nanoseconds
    pub entity_id: u128,      // 128-bit entity identifier (UUID / ID)
    pub payload_size: u32,    // Length of body in bytes
    pub schema_version: u16,  // Version of payload schema
    pub flags: u16,           // Bit 0: Tombstone / Deletion event
    pub record_crc: u32,      // CRC32-C of header + payload
}
```

---

## 3. Concurrency & Memory Architecture

### 3.1 Single-Writer Multi-Reader (SWMR) Model
* **The Writer:**
  * Only one operating system thread ever holds the write lease.
  * Writes pass through a lock-free Single-Producer Single-Consumer (SPSC) ring buffer.
  * Batch commits: The writer groups pending records into 4 KiB buffers and flushes them to disk using `pwrite64` with `fsync` or `O_DSYNC`.
* **The Readers:**
  * Lock-free, completely non-blocking.
  * Readers access the file via read-only memory map (`mmap` with `MAP_SHARED`).
  * Readers never read past the atomic `committed_lsn` published by the writer.

### 3.2 In-Memory Sparse Index Structure
To support sub-millisecond queries without scanning the entire disk file:
* An in-memory concurrent radix tree (or ART - Adaptive Radix Tree) maps `(entity_id) -> Vec<SparseIndexEntry>`.
* Each `SparseIndexEntry` contains:
  ```rust
  pub struct SparseIndexEntry {
      pub timestamp_ns: u64,
      pub lsn: u64,
      pub file_offset: u64,
  }
  ```
* Because entries for any given entity are ordered chronologically by design, finding the state at `timestamp_t` reduces to a standard binary search / `partition_point` over the entity's entry slice.

---

## 4. Query Engine & Time-Travel API

### 4.1 Core Operations
```rust
pub trait ChronoEngine {
    /// Appends a new immutable event. Returns the assigned monotonic LSN.
    fn append(&self, entity_id: u128, payload: &[u8]) -> Result<u64, ChronoError>;

    /// Appends a batch of events atomically in a single page write.
    fn append_batch(&self, events: &[(u128, &[u8])]) -> Result<Vec<u64>, ChronoError>;

    /// Returns the exact state/record of an entity AS OF a given point in time.
    fn get_as_of(&self, entity_id: u128, timestamp_ns: u64) -> Result<Option<RecordRef>, ChronoError>;

    /// Streams all events for an entity within a time window [from_ns, to_ns].
    fn scan_history(&self, entity_id: u128, from_ns: u64, to_ns: u64) -> Result<RecordIterator, ChronoError>;

    /// Returns the most recent record of an entity.
    fn get_latest(&self, entity_id: u128) -> Result<Option<RecordRef>, ChronoError>;

    /// Flushes all staged pages to durable storage.
    fn sync(&self) -> Result<(), ChronoError>;
}
```

### 4.2 AS-OF Resolution Algorithm
1. Lookup `entity_id` in the Radix Index.
2. If absent, return `None`.
3. Perform binary search over `Vec<SparseIndexEntry>` for `timestamp <= query_timestamp`.
4. If index points to an exact record offset, dereference the memory-mapped pointer:
   * Verify `RecordHeader.record_crc == crc32c(slice)`.
   * If flag contains `TOMBSTONE`, return `None` (record was deleted prior to query time).
   * Return zero-copy `RecordRef { header, payload: &[u8] }`.

---

## 5. Crash-Consistency & Fault Tolerance Guarantees

### 5.1 The Torn-Write Threat
If power is severed while a 4096-byte page is being written to disk, flash memory / physical drives may only write 512 bytes or 2048 bytes, leaving the page in an inconsistent, torn state.

### 5.2 Recovery Protocol on Boot
When `ChronoDB::open("data.cdb")` is called:
1. **Validate Superblock:** Check magic bytes `"CHRONODB"` and verify superblock CRC32. If corrupt, abort with `CorruptSuperblock`.
2. **Sequential Page Scan from Last Checkpoint:**
   * Read Page Header at offset $P$.
   * Compute CRC32-C over the full 4096 bytes.
   * Verify monotonic invariant: `page.epoch == prev_page.epoch + 1` and `page.lsn > prev_page.lsn`.
3. **Truncation of Incomplete Tail:**
   * If a page fails checksum or has incomplete bytes at EOF:
   * Truncate the file back to the last valid 4096-byte boundary using `ftruncate(fd, last_valid_offset)`.
   * Log recovery message: `RECOVER: Truncated N corrupt uncommitted bytes at file tail`.

---

## 6. CLI & Embedded Tooling (`chronoctl`)

The repository includes a companion diagnostic binary `chronoctl`:

```bash
# Display storage engine statistics, total pages, and integrity status
chronoctl inspect path/to/database.cdb

# Query an entity at an exact historical timestamp
chronoctl query path/to/database.cdb --id "018f2d8b-7b00-7c2a-96e0-7984f4f34f71" --as-of "2026-10-18T10:00:00Z"

# Stream live real-time appends like tail -f
chronoctl tail path/to/database.cdb --follow

# Verify CRC32-C checksum on every single page in the database
chronoctl verify path/to/database.cdb --strict
```

---

## 7. Phased Implementation Roadmap

### Phase 1: On-Disk Format & Low-Level I/O (Days 1–4)
- [x] Implement `src/storage/page.rs`: `Superblock`, `PageHeader`, `RecordHeader` structs with zero-copy endianness conversions.
- [x] Implement CRC32-C hardware-accelerated checksum verification (via Intel SSE 4.2 / ARM NEON instructions).
  - SSE 4.2 runtime-dispatched on x86_64, slicing-by-8 software fallback elsewhere; ARM NEON intrinsic path still open.
- [x] Build `src/storage/file.rs`: File creation, pre-allocation via `posix_fallocate`, direct aligned buffer writer.

### Phase 2: Append-Only Write Path & SPSC Ring (Days 5–8)
- [x] Implement lock-free SPSC ring buffer for writer thread dispatch.
- [x] Implement 4096-byte page packer: pack variable-length records, split records across contiguous page boundaries if necessary.
- [x] Implement atomic `fsync` flush protocol and monotonic LSN counter.

### Phase 3: Memory-Mapped Read Path & Index (Days 9–12)
- [x] Implement `mmap` manager (`src/index/mmap.rs`) with automated remap on file expansion.
  - Append-only mapping regions with a bounded tail overlap: existing mappings are never moved or unmapped, so zero-copy slices stay valid; growth inside the slack window reuses the last region. Mapped slack past EOF is never readable (`slice` bounds by the observed file length).
- [x] Build in-memory Radix index (`src/index/radix.rs`).
  - 256 sharded `RwLock`-guarded adaptive radix nodes over `u128` keys (fanout stages 4 → 16 → 48 → 256); each leaf holds the entity's `Vec<SparseIndexEntry>` in ascending `(timestamp_ns, lsn)` order.
- [x] Implement binary search `get_as_of` and range-scan iterators.
  - `Engine::get_as_of` / `get_latest` / `scan_history` plus the `ChronoEngine` trait; queries gate on `committed_lsn` and verify the record CRC on dereference. Single-page payloads are zero-copy `Cow::Borrowed` slices into the mapping; page-spanning payloads are reassembled into `Cow::Owned` (page headers interrupt the byte stream on disk).

### Phase 4: Crash Recovery & Corruption Repair (Days 13–15)
- [x] Build startup recovery scanner: validation, tail truncation, and index reconstruction from disk.
  - `Engine::open`/`open_with` runs the validated scan (page CRC, epochs, LSN/timestamp continuity), cuts a torn or corrupt tail back to the last valid page boundary (`OpenOptions { repair }`), rebuilds the sparse index from the surviving records in one pass, and resumes the LSN counter, timestamp clock and publication cursors (`WriterResume` / `ChainState`). `OpenReport` reports records, pages, LSN/timestamp high-water marks and truncated bytes.
  - A trailing sub-page remnant (< 4096 bytes) is cut as part of the same repair; pre-allocated zero pages are left in place and reused.
- [x] Build `chronoctl` CLI utility for inspection and verification.
  - `inspect` / `verify [--repair]` / `tail` implemented; `query` resolves an entity as-of offline by scanning (the live in-process path is the Phase 3 index).
  - `recover <file>` exercises the boot path end to end (validate → truncate → index rebuild → resume → close) and prints the SPEC §5.2 recovery log line.
  - Integration coverage: `tests/recovery.rs` (clean/abandoned/torn/bit-flipped/partial-tail/spanning/tombstone/pre-allocated cases) and `tests/cli_recover.rs`.

### Phase 5: Verification & Adversarial Fuzzing (Days 16–18)
- [x] Build Power-Loss Simulator test harness: spawn writer, send `SIGKILL` or simulate torn-writes with randomized bit-flips, verify recovery never loses previously acknowledged transactions.
  - `tests/crash_recovery.rs`: a child process (test binary re-exec, no `fork`) appends under `PerBatch` durability and is `SIGKILL`ed mid-append — every acknowledged LSN must survive recovery byte-exact (*completeness*); a 120-case fuzz loop then cuts the tail at a random offset, cuts page-aligned, or flips random bits past the superblock and asserts recovery yields a contiguous byte-exact prefix of acknowledged records, never inventing one (*soundness*), while superblock damage refuses to open and leaves the file untouched.
  - The fuzz loop found and fixed two real scanner bugs: in-flight spanning fragments were reported as completed records, and cutting a torn trailing record left a stale page high-water mark, so a resumed writer later failed the strict scan with a "non-zero page after a free page" hole. The scanner now reports completed records only and rolls the page cursor back to the damaged record's first fragment page.
- [x] Run benchmark suite comparing read latency against SQLite and DuckDB.
  - `chronodb readbench` builds a fresh dataset (default 200k records × 64 B over 1k entities, batches of 4096) and measures as-of point queries, latest lookups and per-entity history scans with per-call percentiles, emitting the dataset and the exact query sets as TSV plus a JSON summary. `bench/compare_read.py` replays those same query sets against SQLite (stdlib `sqlite3`, defaults and 256 MiB `mmap_size`) and DuckDB (isolated venv), see `bench/README.md` for methodology and caveats.
  - Results (release build, tmpfs, 200k records / 1k entities, 50k as-of queries, all 50k resolved with CRC-verified records): as-of p50/p99 **0.50 µs / 2.08 µs** (SQLite 6.28/18.78 µs, SQLite+mmap 5.16/15.44 µs, DuckDB 1.37/2.50 ms); latest p50 0.16 µs vs 5.11 µs / 4.61 µs / 1.14 ms; entity scan (200 records) p50 23.6 µs vs 241 µs / 131 µs / 1.19 ms. DuckDB's planner serves this workload with a sequential scan (verified with `EXPLAIN ANALYZE`; the ART index is only consulted for covered index lookups), so it is reported for completeness, not as a point-read baseline.

---

## 8. Verification & Test Plan

```rust
#[cfg(test)]
mod tests {
    // 1. Monotonicity Test
    #[test]
    fn test_monotonic_lsn_and_timestamp();

    // 2. Exact Time Travel Test
    #[test]
    fn test_point_in_time_query_accuracy() {
        // Append version 1 at T=100
        // Append version 2 at T=200
        // Assert as_of(T=150) == version 1
        // Assert as_of(T=250) == version 2
        // Assert as_of(T=50) == None
    }

    // 3. Torn Write Recovery Test
    #[test]
    fn test_torn_write_recovery_truncation() {
        // Write 10 valid pages
        // Append 1200 bytes of garbage (torn write)
        // Re-open DB
        // Assert exactly 10 pages recognized; DB operable without errors
    }
}
```
