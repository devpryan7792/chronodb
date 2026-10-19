//! The read path (SPEC §3.1/§3.2/§4): a memory-mapped, read-only view of the
//! database file plus an in-memory sparse index that turns
//! `(entity_id, timestamp_ns)` lookups into direct record offsets.
//!
//! * [`MmapManager`] maps the file with `MAP_SHARED` in append-only regions
//!   and never moves or unmaps existing mappings, so zero-copy record slices
//!   stay valid for as long as the engine lives.
//! * [`EntityIndex`] is a sharded adaptive radix tree keyed by `u128` entity
//!   ids; each leaf holds the entity's [`SparseIndexEntry`] list in
//!   ascending `(timestamp_ns, lsn)` order, ready for binary search.

pub mod mmap;
pub mod radix;

pub use mmap::MmapManager;
pub use radix::{EntityIndex, SparseIndexEntry};
