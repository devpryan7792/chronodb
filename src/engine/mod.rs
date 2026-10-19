//! The engine facade: [`Engine`] wraps the write pipeline (SPEC §3) — the
//! ack machinery in [`ring`] with a [`PageWriter`] owned by one writer
//! thread — and the read path (SPEC §4) in [`query`], backed by the
//! memory-mapped file view and sparse index the worker feeds.
//!
//! Thread model: any number of API threads call `append`/`sync`; they
//! serialise on the chain's submit lock (re-deriving the ring's SPSC
//! contract), and the single writer thread is the only code that touches the
//! file. With [`ring::Durability::PerBatch`] an append's ack fires only after
//! the record's pages survived `fdatasync`, so a successful `append` is an
//! authoritative durability statement.

pub mod query;
pub mod ring;

pub use query::{ChronoEngine, HistoryIter, ReadRecord};

use std::path::{Path, PathBuf};
use std::sync::Arc;

use parking_lot::Mutex;

use crate::error::Result;
use crate::index::{EntityIndex, MmapManager, SparseIndexEntry};
use crate::storage::file::{CreateOptions, DbFile};
use crate::storage::page::page_offset;
use crate::storage::scanner::scan_and_validate;
use crate::storage::writer::{PageWriter, WriterResume};
use ring::{Chain, ChainState, ChannelOptions};

/// Options for [`Engine::create`].
#[derive(Clone, Debug)]
pub struct EngineOptions {
    /// Write-channel tunables (ring, acks, batcher, durability policy).
    pub channel: ChannelOptions,
    /// Pages pre-allocated on file creation (`0` disables, leaving a sparse
    /// file that grows as pages are written). Ignored by [`Engine::open`].
    pub preallocate_pages: u64,
}

impl Default for EngineOptions {
    fn default() -> Self {
        Self {
            channel: ChannelOptions::default(),
            preallocate_pages: 256,
        }
    }
}

/// Options for [`Engine::open`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct OpenOptions {
    /// Repair a torn or corrupt tail by truncating back to the last valid
    /// page boundary (SPEC §5.2) instead of failing with the corruption
    /// error. The bytes cut are reported in [`OpenReport::truncated_bytes`].
    pub repair: bool,
}

impl Default for OpenOptions {
    fn default() -> Self {
        Self { repair: true }
    }
}

/// Diagnostics from a recovery open ([`Engine::open_with`]).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct OpenReport {
    /// True when every byte of the file validated and nothing was cut.
    pub clean: bool,
    /// Bytes truncated from the tail (torn page writes plus a trailing
    /// sub-page remnant); `0` when clean.
    pub truncated_bytes: u64,
    /// Logical records recovered (spanning records counted once).
    pub record_count: u64,
    /// Data pages recovered (the superblock is page 0).
    pub data_pages: u64,
    /// Highest recovered LSN (`0` for an empty file).
    pub last_lsn: u64,
    /// Highest recovered timestamp (`0` for an empty file).
    pub last_ts: u64,
    /// Trailing all-zero pages left in place: free pre-allocation space.
    pub free_tail_pages: u64,
}

/// Snapshot reported when an engine shuts down.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct EngineStats {
    /// Highest LSN assigned to a record (whether or not it reached disk).
    pub committed_lsn: u64,
    /// Highest LSN that survived `fdatasync`.
    pub durable_lsn: u64,
    /// Pages written to the file (the superblock is page 0).
    pub pages_flushed: u64,
}

/// A single-writer, append-only timestamped record store.
pub struct Engine {
    chain: Arc<Chain>,
    worker: Mutex<Option<std::thread::JoinHandle<()>>>,
    path: PathBuf,
    index: Arc<EntityIndex>,
    mmap: Arc<MmapManager>,
}

impl Engine {
    /// Creates a new database file and starts the writer thread.
    pub fn create(path: &Path, opts: EngineOptions) -> Result<Self> {
        let create = CreateOptions {
            preallocate_pages: opts.preallocate_pages,
            strict_direct_io: false,
        };
        let (db, _sb) = DbFile::create(path, &create)?;
        let writer = PageWriter::create_for(db, opts.channel.writer)?;
        let chain = Arc::new(Chain::new(&opts.channel));
        Self::start(path, chain, writer, opts, Arc::new(EntityIndex::new()))
    }

    /// Reopens an existing database for reads and writes (recovery open,
    /// the [`OpenOptions::default`] behaviour of [`Engine::open_with`]).
    pub fn open(path: &Path, opts: EngineOptions) -> Result<Self> {
        Ok(Self::open_with(path, opts, OpenOptions::default())?.0)
    }

    /// Reopens an existing database: validates every page (CRC, epochs,
    /// LSN/timestamp continuity), cuts a torn or corrupt tail when
    /// `open_opts.repair` is set (SPEC §5.2), rebuilds the sparse index from
    /// the surviving records and resumes LSN/timestamp assignment where the
    /// file left off. With `repair = false` the first corruption is an
    /// error and the file is left untouched.
    pub fn open_with(
        path: &Path,
        opts: EngineOptions,
        open_opts: OpenOptions,
    ) -> Result<(Self, OpenReport)> {
        let (db, _sb) = DbFile::open(path)?;

        // Recovery scan: validate + rebuild the sparse index in one pass.
        // Records arrive in LSN order, so each entity's entries are inserted
        // in ascending timestamp order (the index's ordering contract).
        // Timestamps are globally monotonic, so the last one seen is the max.
        let index = Arc::new(EntityIndex::new());
        let mut last_ts = 0u64;
        let report = scan_and_validate(&db, open_opts.repair, |view| {
            let ts = view.timestamp_ns();
            if ts > last_ts {
                last_ts = ts;
            }
            index.insert(
                view.entity_id(),
                SparseIndexEntry {
                    timestamp_ns: ts,
                    lsn: view.lsn(),
                    file_offset: page_offset(view.start_page) + view.start_offset as u64,
                },
            );
            Ok(())
        })?;

        let last_lsn = report.max_lsn.unwrap_or(0);
        let last_page = report.pages.last().copied();
        let writer = PageWriter::resume_for(
            db,
            opts.channel.writer,
            WriterResume {
                last_lsn,
                next_page: last_page.map_or(1, |p| p.page_index + 1),
                prev_page_max_lsn: last_page.map_or(0, |p| p.max_lsn),
            },
        )?;
        let chain = Arc::new(Chain::with_state(
            &opts.channel,
            ChainState {
                committed_lsn: last_lsn,
                durable_lsn: last_lsn,
                pages_flushed: report.pages.len() as u64,
                last_ts,
            },
        ));

        let engine = Self::start(path, chain, writer, opts, index)?;
        let open_report = OpenReport {
            clean: report.truncated_bytes.is_none(),
            truncated_bytes: report.truncated_bytes.unwrap_or(0),
            record_count: report.record_count,
            data_pages: report.pages.len() as u64,
            last_lsn,
            last_ts,
            free_tail_pages: report.free_tail_pages,
        };
        Ok((engine, open_report))
    }

    /// Wires the mmap view, sparse index and writer thread around `writer`.
    fn start(
        path: &Path,
        chain: Arc<Chain>,
        writer: PageWriter,
        opts: EngineOptions,
        index: Arc<EntityIndex>,
    ) -> Result<Self> {
        // Read path: an mmap view plus a sparse index, both fed by the
        // worker. The mapping tail overlap must cover the largest possible
        // *file extent* of a record (page headers included) so records
        // never straddle mapping regions (index::mmap).
        let overlap =
            crate::storage::writer::max_record_extent(opts.channel.writer.max_record_payload);
        let mmap = Arc::new(MmapManager::open(path, overlap)?);
        mmap.refresh()?; // superblock / recovered pages are readable now
        let read = ring::ReadSide {
            index: Arc::clone(&index),
            mmap: Arc::clone(&mmap),
        };

        let worker = ring::spawn_worker(chain.clone(), writer, opts.channel, read)?;
        Ok(Self {
            chain,
            worker: Mutex::new(Some(worker)),
            path: path.to_path_buf(),
            index,
            mmap,
        })
    }

    /// Appends a record stamped from the wall clock (clamped to the engine's
    /// strictly monotonic sequence) and returns its LSN.
    pub fn append(&self, entity_id: u128, payload: &[u8]) -> Result<u64> {
        self.chain.submit_append(entity_id, 0, 0, payload, None)
    }

    /// Appends with a caller-supplied timestamp, which must be strictly
    /// greater than every timestamp already accepted by this engine.
    pub fn append_at(&self, entity_id: u128, timestamp_ns: u64, payload: &[u8]) -> Result<u64> {
        self.chain
            .submit_append(entity_id, 0, 0, payload, Some(timestamp_ns))
    }

    /// Appends with explicit schema version and record flags.
    pub fn append_with(
        &self,
        entity_id: u128,
        timestamp_ns: u64,
        payload: &[u8],
        schema_version: u16,
        flags: u16,
    ) -> Result<u64> {
        self.chain.submit_append(
            entity_id,
            schema_version,
            flags,
            payload,
            Some(timestamp_ns),
        )
    }

    /// Submits `(entity_id, payload)` pairs as batches. Each chunk of at most
    /// `ChannelOptions::max_batch_records` records is handled atomically by
    /// the worker and acknowledged together; the returned LSNs line up with
    /// the input order. An empty batch is a no-op returning no LSNs.
    pub fn append_batch(&self, batch: &[(u128, &[u8])]) -> Result<Vec<u64>> {
        self.chain.submit_batch(batch)
    }

    /// Authoritative durability barrier: resolves once every record submitted
    /// so far has survived `fdatasync`, and returns the durable LSN.
    pub fn sync(&self) -> Result<u64> {
        self.chain.submit_sync()
    }

    /// Highest LSN assigned by the writer so far.
    pub fn committed_lsn(&self) -> u64 {
        self.chain.committed_lsn()
    }

    /// Highest LSN that survived `fdatasync` so far.
    pub fn durable_lsn(&self) -> u64 {
        self.chain.durable_lsn()
    }

    /// Path of the backing file.
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Sparse index handle (diagnostics/tests).
    pub fn index(&self) -> &EntityIndex {
        &self.index
    }

    /// Memory-mapped file view (diagnostics/tests).
    pub fn mmap(&self) -> &MmapManager {
        &self.mmap
    }

    /// Drains, performs the final flush and (when `final_sync`) the final
    /// `fdatasync`, joins the writer and reports the terminal LSNs. A writer
    /// failure is surfaced here.
    pub fn close(self, final_sync: bool) -> Result<EngineStats> {
        let mut this = self;
        this.shutdown(final_sync)
    }

    /// Closes *without* the final `fdatasync`, discarding anything not yet
    /// synced (used by durability tests; records may still be in the page
    /// cache if an earlier sync covered them).
    pub fn abandon(self) -> Result<EngineStats> {
        let mut this = self;
        this.shutdown(false)
    }

    fn shutdown(&mut self, final_sync: bool) -> Result<EngineStats> {
        if !final_sync {
            self.chain.set_abandoning();
        }
        self.chain.submit_close();
        if let Some(handle) = self.worker.lock().take() {
            let _ = handle.join();
        }
        let stats = EngineStats {
            committed_lsn: self.chain.committed_lsn(),
            durable_lsn: self.chain.durable_lsn(),
            pages_flushed: self.chain.pages_flushed(),
        };
        match self.chain.take_failure() {
            Some(e) => Err(e),
            None => Ok(stats),
        }
    }
}

impl Drop for Engine {
    /// Best-effort close with final sync; a second `close`/`abandon` call is
    /// a no-op because the worker handle is only taken once.
    fn drop(&mut self) {
        let _ = self.shutdown(true);
    }
}
