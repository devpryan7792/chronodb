//! Lock-free SPSC ring buffer and the single-writer dispatch thread.
//!
//! **[`SpscRing`]** — byte-oriented single-producer / single-consumer ring.
//! A frame is `{u32 body_len, u32 kind, body}` with the header and body
//! padded to 8-byte multiples, so a frame never straddles the wrap; when the
//! tail would overflow the buffer end the producer publishes a `PAD` frame
//! first and continues at index 0. The cursor protocol is `Acquire`/`Release`
//! atomics; the ring data path is lock-free by construction.
//!
//! **[`Chain`]/[`spawn_worker`]** — the write channel: API threads serialise
//! submission through `parking_lot::Mutex` (re-establishing the ring's
//! single-producer property for arbitrary callers), acks live in a cache-page
//! slab, and the worker runs
//! `drain batch → pack records into pages → positioned write → fdatasync →
//! publish committed_lsn/durable_lsn → signal acks`.
//!
//! Ack semantics:
//! * `RECORD` — the LSN is acknowledged once every frame of the same batch
//!   has been packed **and** (with [`Durability::PerBatch`]) `fdatasync`ed.
//! * `SYNC` — barrier; the ack slot carries the durable LSN.
//! * `CLOSE` — the worker drains, performs the final flush (+`fdatasync`
//!   unless abandoned) and exits.

use std::cell::UnsafeCell;
use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use byteorder::ByteOrder;
use byteorder::LittleEndian as LE;
use parking_lot::{Condvar, Mutex};

use crate::error::{ChronoError, Result};
use crate::index::{EntityIndex, MmapManager, SparseIndexEntry};
use crate::storage::file::now_ns;
use crate::storage::writer::{AppendOutcome, PageWriter, StagedRecord, WriterOptions};

// ---------------------------------------------------------------------------
// frames
// ---------------------------------------------------------------------------

/// Frame kinds flowing through the ring.
pub mod frame {
    /// Body is a [`RecordFrame`] wire image.
    pub const RECORD: u32 = 1;
    /// No body: the worker ends its batch here and flushes what it holds.
    pub const BATCH_END: u32 = 2;
    /// Body is `[u64 ticket]`: authoritative flush barrier.
    pub const SYNC: u32 = 3;
    /// No body: the worker drains, finalises and exits.
    pub const CLOSE: u32 = 4;
    /// Padding when the tail would straddle the wrap.
    pub const PAD: u32 = 5;
}

const FRAME_HEADER: usize = 8;

#[inline]
const fn align8(n: usize) -> usize {
    (n + 7) & !7
}

/// A submitted append request, as carried inside the ring.
#[derive(Clone, Copy, Debug)]
pub struct RecordFrame<'b> {
    /// Slab index carrying the ack.
    pub ticket: u64,
    /// Engine-global strictly monotonic timestamp (already validated).
    pub timestamp_ns: u64,
    pub entity_id: u128,
    pub schema_version: u16,
    pub flags: u16,
    pub payload: &'b [u8],
}

impl RecordFrame<'_> {
    /// Wire image: `ticket u64 | ts u64 | entity u128 | payload_len u32 |
    /// schema u16 | flags u16 | payload`.
    pub const WIRE_HEADER: usize = 40;

    pub fn encode_into(&self, out: &mut [u8]) {
        debug_assert_eq!(out.len(), self.wire_len());
        LE::write_u64(&mut out[0..8], self.ticket);
        LE::write_u64(&mut out[8..16], self.timestamp_ns);
        LE::write_u128(&mut out[16..32], self.entity_id);
        LE::write_u32(&mut out[32..36], self.payload.len() as u32);
        LE::write_u16(&mut out[36..38], self.schema_version);
        LE::write_u16(&mut out[38..40], self.flags);
        out[40..40 + self.payload.len()].copy_from_slice(self.payload);
    }

    #[inline]
    pub fn wire_len(&self) -> usize {
        Self::WIRE_HEADER + self.payload.len()
    }

    /// Decodes a frame body into a borrowed view.
    pub fn decode<'b>(body: &'b [u8]) -> Result<RecordFrame<'b>> {
        if body.len() < Self::WIRE_HEADER {
            return Err(ChronoError::CorruptRecord {
                offset: 0,
                reason: format!(
                    "request frame too small: {} bytes (need {})",
                    body.len(),
                    Self::WIRE_HEADER
                ),
            });
        }
        let n = LE::read_u32(&body[32..36]) as usize;
        if body.len() < Self::WIRE_HEADER + n {
            return Err(ChronoError::CorruptRecord {
                offset: 0,
                reason: "request frame payload cut short".to_string(),
            });
        }
        Ok(RecordFrame {
            ticket: LE::read_u64(&body[0..8]),
            timestamp_ns: LE::read_u64(&body[8..16]),
            entity_id: LE::read_u128(&body[16..32]),
            schema_version: LE::read_u16(&body[36..38]),
            flags: LE::read_u16(&body[38..40]),
            payload: &body[40..40 + n],
        })
    }

    /// View of the request as a [`StagedRecord`] for the packer.
    pub fn as_staged(&self) -> StagedRecord<'_> {
        StagedRecord {
            entity_id: self.entity_id,
            timestamp_ns: self.timestamp_ns,
            payload: self.payload,
            schema_version: self.schema_version,
            flags: self.flags,
        }
    }
}

// ---------------------------------------------------------------------------
// the SPSC ring
// ---------------------------------------------------------------------------

/// Lock-free SPSC byte ring. `write_frame` is producer-only, `drain`
/// consumer-only; both access disjoint byte ranges enforced by the cursor
/// protocol (`tail - head ≤ capacity`), which is the standard safety argument
/// for this data structure.
pub struct SpscRing {
    buf: UnsafeCell<Box<[u8]>>,
    /// `capacity() - 1`
    mask: usize,
    head: AtomicUsize,
    tail: AtomicUsize,
}

// SAFETY: the data path is a classic SPSC ring; producer and consumer touch
// disjoint byte ranges whose disjointness follows from the cursor protocol
// (`tail - head ≤ capacity`), ordered by Acquire/Release on `head`/`tail`.
unsafe impl Sync for SpscRing {}

impl SpscRing {
    /// Capacity rounded up to the next power of two ≥ `max(min_bytes, 64)`,
    /// so the wrap logic is exercised even in tiny unit tests.
    pub fn with_capacity_pow2(min_bytes: usize) -> Self {
        let cap = 64usize.max(min_bytes).next_power_of_two();
        let buf = vec![0u8; cap].into_boxed_slice();
        Self {
            buf: UnsafeCell::new(buf),
            mask: cap - 1,
            head: AtomicUsize::new(0),
            tail: AtomicUsize::new(0),
        }
    }

    #[inline]
    pub fn capacity(&self) -> usize {
        self.mask + 1
    }

    /// Bytes one frame occupies: header plus 8-aligned body.
    #[inline]
    pub const fn frame_bytes(body_len: usize) -> usize {
        FRAME_HEADER + align8(body_len)
    }

    #[inline]
    pub fn available(&self) -> usize {
        let head = self.head.load(Ordering::Acquire);
        let tail = self.tail.load(Ordering::Acquire);
        self.capacity().saturating_sub(tail.wrapping_sub(head))
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.head.load(Ordering::Acquire) == self.tail.load(Ordering::Acquire)
    }

    // -- producer (exactly one caller at a time — the engine serialises) ------

    /// Tries to publish one `kind` frame. `false` ⇒ ring full; publish again
    /// once the consumer advanced `head`.
    pub fn write_frame(&self, kind: u32, body: &[u8]) -> bool {
        let total = Self::frame_bytes(body.len());
        if total > self.capacity() {
            return false; // can never fit, not even alone
        }
        let head = self.head.load(Ordering::Acquire);
        let mut tail = self.tail.load(Ordering::Relaxed);

        let used = tail.wrapping_sub(head);
        let avail = self.capacity().saturating_sub(used);
        if avail < total {
            return false;
        }

        // Wrap handling: publish a PAD frame and restart at index 0 first.
        let mut idx = tail & self.mask;
        let contig = self.capacity() - idx;
        if idx != 0 && total > contig {
            // Room check *including* the PAD frame; deferring the wrap when
            // the payload does not fit behind it keeps head/tail exact.
            if self.capacity().saturating_sub(used + contig) < total {
                return false;
            }
            debug_assert!(contig >= FRAME_HEADER);
            let padded = contig - FRAME_HEADER;
            let buf: &mut [u8] = unsafe { &mut *self.buf.get() };
            LE::write_u32(&mut buf[idx..idx + 4], padded as u32);
            LE::write_u32(&mut buf[idx + 4..idx + FRAME_HEADER], frame::PAD);
            tail = tail.wrapping_add(contig);
            idx = 0;
            self.tail.store(tail, Ordering::Release);
        }

        let buf: &mut [u8] = unsafe { &mut *self.buf.get() };
        LE::write_u32(&mut buf[idx..idx + 4], body.len() as u32);
        LE::write_u32(&mut buf[idx + 4..idx + FRAME_HEADER], kind);
        let body_at = idx + FRAME_HEADER;
        buf[body_at..body_at + body.len()].copy_from_slice(body);
        // alignment padding is zeroed so partial reads stay deterministic
        let pad_end = (body_at + align8(body.len())).min(self.capacity());
        buf[body_at + body.len()..pad_end].fill(0);

        self.tail.store(tail.wrapping_add(total), Ordering::Release);
        true
    }

    // -- consumer (exactly one caller: the writer worker) ----------------------

    /// Drains frames until the ring is empty, `max_frames` data frames were
    /// handled or `f` returned `false`. PAD frames are skipped and do not
    /// count against `max_frames`. Returns the number of data frames handled.
    pub fn drain<F>(&self, max_frames: usize, f: &mut F) -> Result<usize>
    where
        F: FnMut(u32, &[u8]) -> Result<bool>,
    {
        let mut handled: usize = 0;
        loop {
            let head = self.head.load(Ordering::Relaxed);
            let tail = self.tail.load(Ordering::Acquire);
            if head == tail || handled >= max_frames {
                return Ok(handled);
            }
            let idx = head & self.mask;
            // The frame bytes at head..head+len were published before `tail`
            // moved; only the consumer (this thread) will touch them until
            // head advances, so reading the disjoint slot through a raw
            // pointer is safe.
            let base: *const u8 = unsafe { (*self.buf.get()).as_ptr() };
            let header = unsafe { std::slice::from_raw_parts(base.add(idx), FRAME_HEADER) };
            let body_len = LE::read_u32(&header[0..4]) as usize;
            let kind = LE::read_u32(&header[4..FRAME_HEADER]);
            if kind == frame::PAD {
                self.head.store(
                    head.wrapping_add(FRAME_HEADER + body_len),
                    Ordering::Release,
                );
                continue;
            }
            let body =
                unsafe { std::slice::from_raw_parts(base.add(idx + FRAME_HEADER), body_len) };
            let proceed = f(kind, body)?;
            self.head.store(
                head.wrapping_add(FRAME_HEADER + align8(body_len)),
                Ordering::Release,
            );
            handled += 1;
            if !proceed {
                return Ok(handled);
            }
        }
    }
}

// ---------------------------------------------------------------------------
// ack slab
// ---------------------------------------------------------------------------

const ACK_PENDING: u32 = 0;
const ACK_DONE: u32 = 1;
const ACK_FAILED: u32 = 2;

/// Stored error codes for failed acks.
pub const ERR_TOO_LARGE: u32 = 1;
pub const ERR_NON_MONOTONIC_TS: u32 = 2;
pub const ERR_IO: u32 = 3;
pub const ERR_CLOSED: u32 = 4;

/// Auxiliary word 1 (e.g. rejected payload size).
/// Auxiliary word 2 (e.g. configured payload cap).
struct AckSlot {
    /// RECORD: assigned LSN. SYNC: durable LSN.
    lsn: AtomicU64,
    state: AtomicU32,
    err: AtomicU32,
    aux: AtomicU32,
    aux2: AtomicU32,
}

impl AckSlot {
    fn fresh() -> Self {
        Self {
            lsn: AtomicU64::new(0),
            state: AtomicU32::new(ACK_DONE),
            err: AtomicU32::new(0),
            aux: AtomicU32::new(0),
            aux2: AtomicU32::new(0),
        }
    }

    /// Marks the slot as awaiting the worker.
    fn reset(&self) {
        self.lsn.store(0, Ordering::Relaxed);
        self.err.store(0, Ordering::Relaxed);
        self.aux.store(0, Ordering::Relaxed);
        self.aux2.store(0, Ordering::Relaxed);
        self.state.store(ACK_PENDING, Ordering::Release);
    }

    /// Completes the slot. `Err((err, aux, aux2))` becomes ACK_FAILED.
    fn complete(&self, outcome: Result<u64, (u32, u32, u32)>) {
        match outcome {
            Ok(lsn) => {
                self.lsn.store(lsn, Ordering::Relaxed);
                self.state.store(ACK_DONE, Ordering::Release);
            }
            Err((code, aux, aux2)) => {
                self.lsn.store(0, Ordering::Relaxed);
                self.aux.store(aux, Ordering::Relaxed);
                self.aux2.store(aux2, Ordering::Relaxed);
                self.err.store(code, Ordering::Relaxed);
                self.state.store(ACK_FAILED, Ordering::Release);
            }
        }
    }

    fn map_code(code: u32, aux: u32, aux2: u32) -> ChronoError {
        match code {
            ERR_TOO_LARGE => ChronoError::RecordTooLarge {
                size: aux as usize,
                max: aux2 as usize,
            },
            ERR_NON_MONOTONIC_TS => ChronoError::NonMonotonicTimestamp {
                previous: aux2 as u64,
                current: aux as u64,
            },
            ERR_IO => ChronoError::WriterFailed(
                "the writer worker hit an i/o error and stopped".to_string(),
            ),
            _ => ChronoError::EngineClosed,
        }
    }
}

// ---------------------------------------------------------------------------
// chain (shared state)
// ---------------------------------------------------------------------------

/// Where `sync_data(2)` runs in the pipeline.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Durability {
    /// Every drained batch is page-written and fdatasync'd before its acks
    /// fire: `append` returns durable LSNs (SPEC §3.1).
    PerBatch,
    /// Acks fire once records are packed and their pages written (page
    /// cache); durability is explicit via `Engine::sync()`.
    OnSync,
}

/// Tunables of the write channel.
#[derive(Clone, Copy, Debug)]
pub struct ChannelOptions {
    /// Ring capacity in bytes (power-of-two rounded). Default 1 MiB.
    pub ring_capacity: usize,
    /// Ack slab size (power-of-two rounded, ≥ 64 slots). Default 8192.
    pub ack_slab: usize,
    /// Upper bound of records handled per worker iteration. Default 4096.
    pub max_batch_records: usize,
    pub durability: Durability,
    /// Idle timeout before the worker writes staged pages in OnSync mode.
    pub idle_flush: Duration,
    pub writer: WriterOptions,
}

impl Default for ChannelOptions {
    fn default() -> Self {
        Self {
            ring_capacity: 1 << 20,
            ack_slab: 8192,
            max_batch_records: 4096,
            durability: Durability::PerBatch,
            idle_flush: Duration::from_millis(5),
            writer: WriterOptions::default(),
        }
    }
}

/// Outcome carried by the worker into a slab slot.
type AckOutcome = Result<u64, (u32, u32, u32)>;

/// Shared channel state: ring + ack slab + cursors + wait queues.
pub struct Chain {
    ring: SpscRing,
    slab: Box<[AckSlot]>,
    slab_mask: usize,
    ack_cursor: AtomicUsize,
    /// Upper bound of data frames handled per worker iteration.
    max_batch_records: usize,

    /// Serialises ring writes (single-producer reconstruction).
    submit_lock: Mutex<()>,
    /// Park mutex for producers waiting on ring space; distinct from
    /// `submit_lock`, which producers hold while pushing.
    space_lock: Mutex<()>,
    /// Wakes producers waiting for ring space (released while parked).
    space_cv: Condvar,
    /// Wakes the worker after submissions / close.
    wake_lock: Mutex<()>,
    wake_cv: Condvar,
    /// Wakes ack waiters after worker batches complete.
    ack_lock: Mutex<()>,
    ack_cv: Condvar,

    closed: AtomicBool,
    worker_done: AtomicBool,
    /// Set when the caller wants a close *without* the final fdatasync.
    abandoning: AtomicBool,

    committed_lsn: AtomicU64,
    durable_lsn: AtomicU64,
    pages_flushed: AtomicU64,
    last_ts: AtomicU64,
    /// First fatal error that stopped the worker.
    failure: Mutex<Option<ChronoError>>,
}

/// Published cursors a chain inherits when it is reopened on an existing
/// file: everything recovered from disk is already committed and durable.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ChainState {
    /// Highest LSN already in the file (`0` for an empty file).
    pub committed_lsn: u64,
    /// Highest LSN that survived `fdatasync` (`0` for an empty file).
    pub durable_lsn: u64,
    /// Data pages already written (the superblock is page 0).
    pub pages_flushed: u64,
    /// Highest timestamp already accepted (`0` for an empty file), so
    /// timestamp assignment stays strictly monotonic across a reopen.
    pub last_ts: u64,
}

impl Chain {
    pub fn new(opts: &ChannelOptions) -> Self {
        Self::with_state(opts, ChainState::default())
    }

    /// Builds a chain whose published cursors start at recovered values
    /// instead of zero (used by [`crate::engine::Engine::open`]).
    pub fn with_state(opts: &ChannelOptions, state: ChainState) -> Self {
        let slab_len = opts.ack_slab.max(64).next_power_of_two();
        let slab = (0..slab_len).map(|_| AckSlot::fresh()).collect::<Vec<_>>();
        Self {
            ring: SpscRing::with_capacity_pow2(opts.ring_capacity),
            slab: slab.into_boxed_slice(),
            slab_mask: slab_len - 1,
            ack_cursor: AtomicUsize::new(0),
            max_batch_records: opts.max_batch_records,
            submit_lock: Mutex::new(()),
            space_lock: Mutex::new(()),
            space_cv: Condvar::new(),
            wake_lock: Mutex::new(()),
            wake_cv: Condvar::new(),
            ack_lock: Mutex::new(()),
            ack_cv: Condvar::new(),
            closed: AtomicBool::new(false),
            worker_done: AtomicBool::new(false),
            abandoning: AtomicBool::new(false),
            committed_lsn: AtomicU64::new(state.committed_lsn),
            durable_lsn: AtomicU64::new(state.durable_lsn),
            pages_flushed: AtomicU64::new(state.pages_flushed),
            last_ts: AtomicU64::new(state.last_ts),
            failure: Mutex::new(None),
        }
    }

    // -- published state ------------------------------------------------------

    #[inline]
    pub fn committed_lsn(&self) -> u64 {
        self.committed_lsn.load(Ordering::Acquire)
    }

    #[inline]
    pub fn durable_lsn(&self) -> u64 {
        self.durable_lsn.load(Ordering::Acquire)
    }

    #[inline]
    pub fn pages_flushed(&self) -> u64 {
        self.pages_flushed.load(Ordering::Acquire)
    }

    #[inline]
    pub fn is_closed(&self) -> bool {
        self.closed.load(Ordering::Acquire)
    }

    #[inline]
    pub fn worker_done(&self) -> bool {
        self.worker_done.load(Ordering::Acquire)
    }

    /// Seconds of `last_ts` — exposed for tests.
    #[inline]
    pub fn last_ts(&self) -> u64 {
        self.last_ts.load(Ordering::Acquire)
    }

    /// The first fatal error observed by the worker, rendered for reporting.
    pub fn failure(&self) -> Option<String> {
        let guard = self.failure.lock();
        guard.as_ref().map(|e| e.to_string())
    }

    /// Consumes the recorded failure (single-shot; used by engine shutdown).
    pub fn take_failure(&self) -> Option<ChronoError> {
        self.failure.lock().take()
    }

    /// Marks the engine so `close()` skips the final `fdatasync` (records
    /// stay purely in memory; observable on reopen).
    pub fn set_abandoning(&self) {
        self.abandoning.store(true, Ordering::Release);
    }

    // -- clock ----------------------------------------------------------------

    /// Engine-monotonic clamp: `max(wall_now, last + 1)`.
    #[inline]
    fn stamp_time_next(&self, wall_now: u64) -> u64 {
        let prev = self.last_ts.load(Ordering::Acquire);
        let ts = if wall_now > prev { wall_now } else { prev + 1 };
        self.last_ts.store(ts, Ordering::Release);
        ts
    }

    /// Caller-provided timestamp; must be strictly greater than every
    /// timestamp already committed to this engine.
    #[inline]
    fn stamp_time_exact(&self, ts: u64) -> Result<u64> {
        let prev = self.last_ts.load(Ordering::Acquire);
        if ts <= prev {
            return Err(ChronoError::NonMonotonicTimestamp {
                previous: prev,
                current: ts,
            });
        }
        self.last_ts.store(ts, Ordering::Release);
        Ok(ts)
    }

    // -- the submission path ----------------------------------------------------

    /// Allocates a free slab slot. Only called under `submit_lock`.
    fn take_ticket(&self) -> Result<usize> {
        let idx = self.ack_cursor.fetch_add(1, Ordering::Relaxed) & self.slab_mask;
        let slot = &self.slab[idx];
        let mut spins: u32 = 0;
        loop {
            match slot.state.load(Ordering::Acquire) {
                ACK_DONE | ACK_FAILED => {
                    slot.reset();
                    return Ok(idx);
                }
                _ => {
                    if self.worker_done() {
                        slot.complete(Err((ERR_CLOSED, 0, 0)));
                        return Err(ChronoError::EngineClosed);
                    }
                    std::hint::spin_loop();
                    spins += 1;
                    if spins % 64 == 0 {
                        std::thread::yield_now();
                    }
                }
            }
        }
    }

    /// Publishes one frame; parks on `space_cv` when the ring is full.
    fn push_frame(&self, kind: u32, body: &[u8]) -> Result<()> {
        let need = SpscRing::frame_bytes(body.len());
        if need > self.ring.capacity() {
            return Err(ChronoError::RecordTooLarge {
                size: body.len(),
                max: self.ring.capacity() - SpscRing::frame_bytes(0),
            });
        }
        while !self.ring.write_frame(kind, body) {
            if self.is_closed() || self.worker_done() {
                return Err(ChronoError::EngineClosed);
            }
            // Park on the dedicated space lock: `submit_lock` is held by the
            // caller across the whole push, which is exactly what preserves
            // the ring's single-producer property.
            let mut guard = self.space_lock.lock();
            self.space_cv
                .wait_for(&mut guard, Duration::from_millis(10));
        }
        Ok(())
    }

    fn closed_failure(&self) -> ChronoError {
        match self.failure() {
            Some(msg) => ChronoError::WriterFailed(msg),
            None => ChronoError::EngineClosed,
        }
    }

    // -- public submit entry points (used by the engine facade) --------------

    /// Submits one record; `ts` = `None` uses the wall clock with the
    /// engine's monotonic clamp, `Some(ts)` validates strict monotonicity.
    pub fn submit_append(
        &self,
        entity_id: u128,
        schema_version: u16,
        flags: u16,
        payload: &[u8],
        ts: Option<u64>,
    ) -> Result<u64> {
        let guard = self.submit_lock.lock();
        if self.is_closed() {
            return Err(self.closed_failure());
        }
        let ts = match ts {
            Some(explicit) => self.stamp_time_exact(explicit)?,
            None => self.stamp_time_next(now_ns()),
        };
        let ticket = self.take_ticket()?;
        let frame = RecordFrame {
            ticket: ticket as u64,
            timestamp_ns: ts,
            entity_id,
            schema_version,
            flags,
            payload,
        };
        let mut body = vec![0u8; frame.wire_len()];
        frame.encode_into(&mut body);
        if let Err(e) = self.push_frame(frame::RECORD, &body) {
            self.slab[ticket].complete(Err((ERR_CLOSED, 0, 0)));
            return Err(e);
        }
        drop(guard);
        drop(body);
        // Wake the worker: without this its next drain would wait out the
        // whole idle timeout and every append would pay it as latency.
        self.notify_wake();
        self.wait_ack(ticket)
    }

    /// Submits a batch of `(entity, payload)` pairs atomically relative to
    /// the write pipeline: the worker flushes everything up to the batch end
    /// marker in one position. Batches larger than `max_batch_records` are
    /// chunked (each chunk is one atomic unit). An empty batch is a no-op.
    pub fn submit_batch(&self, items: &[(u128, &[u8])]) -> Result<Vec<u64>> {
        let mut lsns = Vec::with_capacity(items.len());
        for chunk in items.chunks(self.max_batch_records.max(1)) {
            lsns.extend(self.submit_batch_chunk(chunk)?);
        }
        Ok(lsns)
    }

    /// One atomic chunk: records + BATCH_END under a single submit lock.
    fn submit_batch_chunk(&self, chunk: &[(u128, &[u8])]) -> Result<Vec<u64>> {
        let guard = self.submit_lock.lock();
        if self.is_closed() {
            return Err(self.closed_failure());
        }
        let mut tickets = Vec::with_capacity(chunk.len());
        for (entity_id, payload) in chunk {
            let ts = self.stamp_time_next(now_ns());
            let ticket = self.take_ticket()?;
            let frame = RecordFrame {
                ticket: ticket as u64,
                timestamp_ns: ts,
                entity_id: *entity_id,
                schema_version: 0,
                flags: 0,
                payload,
            };
            let mut body = vec![0u8; frame.wire_len()];
            frame.encode_into(&mut body);
            if let Err(e) = self.push_frame(frame::RECORD, &body) {
                self.slab[ticket].complete(Err((ERR_CLOSED, 0, 0)));
                return Err(e);
            }
            tickets.push(ticket);
        }
        if let Err(e) = self.push_frame(frame::BATCH_END, &[]) {
            for t in &tickets {
                self.slab[*t].complete(Err((ERR_CLOSED, 0, 0)));
            }
            return Err(e);
        }
        drop(guard);
        self.notify_wake();

        let mut lsns = Vec::with_capacity(tickets.len());
        for ticket in tickets {
            lsns.push(self.wait_ack(ticket)?);
        }
        Ok(lsns)
    }

    /// Submits the authoritative flush barrier; resolves to the durable LSN.
    pub fn submit_sync(&self) -> Result<u64> {
        let guard = self.submit_lock.lock();
        if self.is_closed() {
            return Err(self.closed_failure());
        }
        let ticket = self.take_ticket()?;
        let mut body = [0u8; 8];
        LE::write_u64(&mut body[..], ticket as u64);
        if let Err(e) = self.push_frame(frame::SYNC, &body) {
            self.slab[ticket].complete(Err((ERR_CLOSED, 0, 0)));
            return Err(e);
        }
        drop(guard);
        self.notify_wake();
        self.wait_ack(ticket)
    }

    /// Ends the pipeline: rejects new submissions and asks the worker to
    /// drain whatever is queued, finalise and exit. Idempotent.
    pub fn submit_close(&self) {
        self.closed.store(true, Ordering::Release);
        self.notify_wake();
        self.notify_space();
    }

    pub fn notify_wake(&self) {
        let _g = self.wake_lock.lock();
        self.wake_cv.notify_all();
    }

    pub fn notify_space(&self) {
        self.space_cv.notify_all();
    }

    // -- acks --------------------------------------------------------------------

    /// Resolves a ticket. Long waits park on `ack_cv` with deadlines so a
    /// wedged worker cannot hang a caller forever, but the fast path is a
    /// short spin (acks complete within one worker batch).
    pub fn wait_ack(&self, ticket: usize) -> Result<u64> {
        let slot = &self.slab[ticket];
        let mut spins: u32 = 0;
        loop {
            match slot.state.load(Ordering::Acquire) {
                ACK_DONE => return Ok(slot.lsn.load(Ordering::Acquire)),
                ACK_FAILED => {
                    let code = slot.err.load(Ordering::Acquire);
                    let aux = slot.aux.load(Ordering::Acquire);
                    let aux2 = slot.aux2.load(Ordering::Acquire);
                    return Err(AckSlot::map_code(code, aux, aux2));
                }
                _ => {}
            }
            if self.worker_done() {
                // No more batches; pending means the worker died mid-flight.
                return Err(self.closed_failure());
            }
            if spins < 2048 {
                std::hint::spin_loop();
                spins += 1;
                continue;
            }
            let mut guard = self.ack_lock.lock();
            self.ack_cv.wait_for(&mut guard, Duration::from_millis(10));
        }
    }

    /// Worker only: completes a ticket and wakes any waiter.
    fn ack(&self, ticket: usize, outcome: AckOutcome) {
        self.slab[ticket].complete(outcome);
        self.ack_cv.notify_all();
    }

    /// Worker only: fails every pending slot (fatal error path).
    fn fail_pending(&self, code: u32) {
        for slot in self.slab.iter() {
            if slot.state.load(Ordering::Acquire) == ACK_PENDING {
                slot.complete(Err((code, 0, 0)));
            }
        }
        self.ack_cv.notify_all();
    }

    fn store_failure(&self, e: ChronoError) {
        let mut guard = self.failure.lock();
        if guard.is_none() {
            *guard = Some(e);
        }
        drop(guard);
        self.fail_pending(ERR_IO);
    }
}

// ---------------------------------------------------------------------------
// the worker
// ---------------------------------------------------------------------------

/// Shared handles the worker feeds once records reached the file, so the
/// read path (mmap + sparse index) only ever observes a strictly
/// post-durability prefix.
#[derive(Clone)]
pub struct ReadSide {
    pub index: Arc<EntityIndex>,
    pub mmap: Arc<MmapManager>,
}

/// One drained record's packing result, kept until the durability step so
/// the ack, the index insert and the LSN publications all follow it.
struct Handled {
    ticket: usize,
    entity_id: u128,
    timestamp_ns: u64,
    outcome: Result<AppendOutcome, (u32, u32, u32)>,
}

/// Spawns the single-writer thread. Takes ownership of the [`PageWriter`].
pub fn spawn_worker(
    chain: Arc<Chain>,
    writer: PageWriter,
    opts: ChannelOptions,
    read: ReadSide,
) -> Result<std::thread::JoinHandle<()>, ChronoError> {
    std::thread::Builder::new()
        .name("chronodb-writer".to_string())
        .spawn(move || worker_loop(chain, writer, opts, read))
        .map_err(|e| {
            ChronoError::Io(std::io::Error::other(format!(
                "failed to spawn writer thread: {e}"
            )))
        })
}

fn worker_loop(chain: Arc<Chain>, mut writer: PageWriter, opts: ChannelOptions, read: ReadSide) {
    let mut fatal: Option<ChronoError> = None;

    'pipeline: loop {
        // Close decision: drain everything already submitted, then finalise.
        if chain.is_closed() && chain.ring.is_empty() {
            break 'pipeline;
        }
        if chain.worker_done() {
            break 'pipeline;
        }

        let mut handled: VecDeque<Handled> = VecDeque::new();
        let mut sync_ticket: Option<usize> = None;
        let mut saw_close = false;

        let drained = chain.ring.drain(opts.max_batch_records, &mut |kind, body| {
            match kind {
                frame::RECORD => {
                    let frame = match RecordFrame::decode(body) {
                        Ok(f) => f,
                        Err(e) => return Err(e),
                    };
                    let ticket = frame.ticket as usize;
                    let result = match writer.append_located(frame.as_staged()) {
                        Ok(outcome) => Ok(outcome),
                        Err(e) => {
                            // Per-record rejections keep the engine alive.
                            if let ChronoError::RecordTooLarge { size, max } = e {
                                Err((ERR_TOO_LARGE, size as u32, max as u32))
                            } else if let ChronoError::NonMonotonicTimestamp { previous, current } =
                                e
                            {
                                Err((ERR_NON_MONOTONIC_TS, current as u32, previous as u32))
                            } else {
                                // Pack/I/O failure is fatal for the engine.
                                return Err(e);
                            }
                        }
                    };
                    handled.push_back(Handled {
                        ticket,
                        entity_id: frame.entity_id,
                        timestamp_ns: frame.timestamp_ns,
                        outcome: result,
                    });
                    Ok(true)
                }
                frame::SYNC => {
                    if body.len() >= 8 {
                        let ticket = LE::read_u64(&body[0..8]) as usize;
                        sync_ticket = Some(ticket);
                        Ok(false)
                    } else {
                        Err(ChronoError::CorruptRecord {
                            offset: 0,
                            reason: "sync frame without ticket".to_string(),
                        })
                    }
                }
                frame::BATCH_END => Ok(false),
                frame::CLOSE => {
                    saw_close = true;
                    Ok(false)
                }
                _ => Ok(true),
            }
        });

        if let Err(e) = drained {
            fatal = Some(e);
            break 'pipeline;
        }

        // Durability step for everything drained in this iteration.
        let sync_requested = sync_ticket.is_some();
        let durability_step: Result<u64> =
            if sync_requested || (opts.durability == Durability::PerBatch && !handled.is_empty()) {
                writer.sync()
            } else if !handled.is_empty() {
                writer.flush()
            } else {
                Ok(writer.durable_lsn())
            };

        if let Err(e) = durability_step {
            fatal = Some(e);
            break 'pipeline;
        }

        // Feed the read path only after the durability step, once record
        // bytes are in the file: refresh the mapping first so an index entry
        // can never exist without its bytes being readable, then index.
        if !handled.is_empty() {
            if let Err(e) = read.mmap.refresh() {
                fatal = Some(e);
                break 'pipeline;
            }
            for h in &handled {
                if let Ok(outcome) = &h.outcome {
                    read.index.insert(
                        h.entity_id,
                        SparseIndexEntry {
                            timestamp_ns: h.timestamp_ns,
                            lsn: outcome.lsn,
                            file_offset: outcome.file_offset,
                        },
                    );
                }
            }
        }

        chain
            .committed_lsn
            .store(writer.last_assigned_lsn(), Ordering::Release);
        if opts.durability == Durability::PerBatch || sync_requested {
            chain
                .durable_lsn
                .store(writer.durable_lsn(), Ordering::Release);
        }
        chain
            .pages_flushed
            .store(writer.flushed_pages(), Ordering::Release);

        for h in handled.drain(..) {
            chain.ack(h.ticket, h.outcome.map(|o| o.lsn));
        }
        // A batch left the ring: producers blocked on space may resume.
        chain.notify_space();

        if let Some(ticket) = sync_ticket.take() {
            let durable = writer.durable_lsn();
            chain.durable_lsn.store(durable, Ordering::Release);
            chain.ack(ticket, Ok(durable));
        }

        if saw_close {
            break 'pipeline;
        }

        // Idle: flush staged pages (page-cache visible) and wait briefly, but
        // only while the ring stays empty — a submission between the check
        // and the park must not sleep out the whole timeout.
        if chain.ring.is_empty() {
            if opts.durability == Durability::OnSync {
                if let Err(e) = writer.flush() {
                    fatal = Some(e);
                    break 'pipeline;
                }
            }
            let deadline = std::time::Instant::now() + opts.idle_flush;
            let mut guard = chain.wake_lock.lock();
            while chain.ring.is_empty() {
                let now = std::time::Instant::now();
                if now >= deadline {
                    break;
                }
                let _ = chain.wake_cv.wait_for(&mut guard, deadline - now);
            }
        }
    }

    // ---- finalisation --------------------------------------------------------
    match fatal {
        Some(e) => {
            // Everything packed but undrained content stops here: pages stay
            // on disk up to the last committed page; recovery (Phase 4) will
            // rebuild the exact prefix.
            let _ = writer.flush();
            chain
                .committed_lsn
                .store(writer.last_assigned_lsn(), Ordering::Release);
            chain.store_failure(e);
        }
        None => {
            // Graceful close: pages + fsync, unless the caller explicitly
            // abandoned durability.
            if !chain.abandoning.load(Ordering::Acquire) {
                if let Err(e) = writer.sync() {
                    chain
                        .committed_lsn
                        .store(writer.last_assigned_lsn(), Ordering::Release);
                    chain.store_failure(e);
                }
            }
            chain
                .committed_lsn
                .store(writer.last_assigned_lsn(), Ordering::Release);
            if !chain.abandoning.load(Ordering::Acquire) {
                chain
                    .durable_lsn
                    .store(writer.durable_lsn(), Ordering::Release);
            }
            chain
                .pages_flushed
                .store(writer.flushed_pages(), Ordering::Release);
        }
    }

    chain.worker_done.store(true, Ordering::Release);
    chain.ack_cv.notify_all();
    chain.wake_cv.notify_all();
    chain.space_cv.notify_all();
}

// ---------------------------------------------------------------------------
// tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn gather(ring: &SpscRing) -> Vec<(u32, Vec<u8>)> {
        let mut out = Vec::new();
        let handled = ring
            .drain(usize::MAX, &mut |kind, body| {
                out.push((kind, body.to_vec()));
                Ok(true)
            })
            .expect("drain");
        assert_eq!(handled, out.len());
        out
    }

    #[test]
    fn frames_roundtrip_across_wrap() {
        // Capacity 64. A 28-byte body occupies 36 bytes (8-byte header plus
        // 8-aligned body). With A in place the next frame cannot fit
        // contiguously, so once A drains the producer must publish a PAD
        // frame and restart at index 0 — exercising wrap and PAD skipping.
        let ring = SpscRing::with_capacity_pow2(40);
        assert_eq!(ring.capacity(), 64);
        let a = vec![0xAAu8; 28];
        let b = vec![0xBBu8; 28];
        assert!(ring.write_frame(frame::RECORD, &a));
        assert!(!ring.write_frame(frame::RECORD, &b)); // no room contiguously
        assert_eq!(gather(&ring), vec![(frame::RECORD, a.clone())]);
        assert!(ring.write_frame(frame::RECORD, &b)); // wraps behind a PAD frame
        assert_eq!(gather(&ring), vec![(frame::RECORD, b)]);
        assert!(ring.is_empty());
    }

    #[test]
    fn zero_length_frames_are_valid() {
        let ring = SpscRing::with_capacity_pow2(64);
        assert!(ring.write_frame(frame::BATCH_END, &[]));
        assert!(ring.write_frame(frame::CLOSE, &[]));
        let drained = gather(&ring);
        assert_eq!(
            drained,
            vec![(frame::BATCH_END, Vec::new()), (frame::CLOSE, Vec::new()),]
        );
    }

    #[test]
    fn drain_stops_at_frame_budget() {
        let ring = SpscRing::with_capacity_pow2(1024);
        for i in 0u32..10 {
            assert!(ring.write_frame(frame::RECORD, &i.to_le_bytes()));
        }
        let mut seen = Vec::new();
        let n = ring
            .drain(3, &mut |_kind, body| {
                seen.push(u32::from_le_bytes(body.try_into().unwrap()));
                Ok(true)
            })
            .unwrap();
        assert_eq!((n, seen), (3, vec![0, 1, 2]));
        let rest = gather(&ring);
        assert_eq!(rest.len(), 7);
        assert_eq!(u32::from_le_bytes(rest[0].1.clone().try_into().unwrap()), 3);
    }

    #[test]
    fn oversize_frame_is_rejected_without_corruption() {
        let ring = SpscRing::with_capacity_pow2(64);
        assert!(!ring.write_frame(frame::RECORD, &[0u8; 60])); // needs 72
        assert!(ring.write_frame(frame::RECORD, &[0u8; 56])); // exactly 64
        assert_eq!(ring.available(), 0);
        let drained = gather(&ring);
        assert_eq!(drained.len(), 1);
        assert_eq!(drained[0].1.len(), 56);
    }

    #[test]
    fn chain_stamps_monotonic_wall_times() {
        let chain = Chain::new(&ChannelOptions::default());
        assert_eq!(chain.stamp_time_next(100), 100);
        // Clock behind the high-water mark: clamped to prev + 1.
        assert_eq!(chain.stamp_time_next(100), 101);
        assert_eq!(chain.stamp_time_next(5), 102);
        assert_eq!(chain.last_ts(), 102);

        assert!(matches!(
            chain.stamp_time_exact(102),
            Err(ChronoError::NonMonotonicTimestamp {
                previous: 102,
                current: 102
            })
        ));
        assert_eq!(chain.stamp_time_exact(1_000).unwrap(), 1_000);
        assert_eq!(chain.stamp_time_exact(2_000).unwrap(), 2_000);
    }

    #[test]
    fn chain_rejects_oversized_bodies() {
        let chain = Chain::new(&ChannelOptions::default());
        let cap = chain.ring.capacity();
        let err = chain
            .push_frame(frame::RECORD, &vec![0u8; cap])
            .unwrap_err();
        assert!(matches!(
            err,
            ChronoError::RecordTooLarge { size, max }
                if size == cap && max == cap - SpscRing::frame_bytes(0)
        ));
        assert!(chain.push_frame(frame::RECORD, &vec![0u8; cap - 8]).is_ok());
    }

    #[test]
    fn chain_empty_batch_is_noop() {
        let chain = Chain::new(&ChannelOptions::default());
        assert!(chain.submit_batch(&[]).unwrap().is_empty());
    }

    #[test]
    fn chain_close_is_idempotent() {
        let chain = Chain::new(&ChannelOptions::default());
        assert!(!chain.is_closed());
        chain.submit_close();
        chain.submit_close();
        assert!(chain.is_closed());
        assert!(matches!(
            chain.submit_sync(),
            Err(ChronoError::EngineClosed)
        ));
    }

    #[test]
    fn ack_codes_map_to_public_errors() {
        assert!(matches!(
            AckSlot::map_code(ERR_TOO_LARGE, 5, 9),
            ChronoError::RecordTooLarge { size: 5, max: 9 }
        ));
        assert!(matches!(
            AckSlot::map_code(ERR_NON_MONOTONIC_TS, 7, 3),
            ChronoError::NonMonotonicTimestamp {
                current: 7,
                previous: 3
            }
        ));
        assert!(matches!(
            AckSlot::map_code(ERR_IO, 0, 0),
            ChronoError::WriterFailed(_)
        ));
        assert!(matches!(
            AckSlot::map_code(ERR_CLOSED, 0, 0),
            ChronoError::EngineClosed
        ));
        assert!(matches!(
            AckSlot::map_code(999, 0, 0),
            ChronoError::EngineClosed
        ));
    }

    #[test]
    fn ack_slot_lifecycle() {
        let chain = Chain::new(&ChannelOptions::default());
        let ticket = chain.take_ticket().unwrap();
        assert_eq!(
            chain.slab[ticket].state.load(Ordering::Acquire),
            ACK_PENDING
        );
        chain.ack(ticket, Ok(41));
        assert_eq!(chain.wait_ack(ticket).unwrap(), 41);

        let ticket = chain.take_ticket().unwrap();
        chain.ack(ticket, Err((ERR_TOO_LARGE, 2, 3)));
        assert!(matches!(
            chain.wait_ack(ticket),
            Err(ChronoError::RecordTooLarge { size: 2, max: 3 })
        ));
    }
}
