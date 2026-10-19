//! Sharded adaptive radix tree mapping `u128` entity ids to sparse index
//! entries (SPEC §3.2 "In-Memory Sparse Index").
//!
//! Structure:
//! * the top byte of the key selects one of 256 shards, each an
//!   [`RwLock`]-protected root — readers of different id spaces never
//!   contend, and readers of the same shard only contend with the rare
//!   writer batch;
//! * below the shard, an adaptive radix node fans out per key byte with
//!   growing capacity stages (4 → 16 → 48 → 256, ART's Node4/16/48/256);
//! * a node reached after all 16 key bytes carries the entity's entry list.
//!
//! Entry lists are appended in LSN order and the engine's strictly
//! monotonic timestamp discipline keeps them ordered by `timestamp_ns`, so
//! as-of resolution is a `partition_point` binary search.

use std::sync::atomic::{AtomicU64, Ordering};

use parking_lot::RwLock;

/// Key length in bytes: entity ids are `u128`.
const KEY_BYTES: usize = 16;
/// Shard count: one per possible top byte.
const SHARDS: usize = 256;
/// Fanout capacity stages of an adaptive node (ART node kinds).
const FANOUT_STAGES: [usize; 4] = [4, 16, 48, 256];

/// One record's position, keyed by its timestamp (SPEC §3.2).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SparseIndexEntry {
    /// Record timestamp in nanoseconds (the ordering key).
    pub timestamp_ns: u64,
    /// Globally contiguous log sequence number.
    pub lsn: u64,
    /// Absolute byte offset of the record header in the database file.
    pub file_offset: u64,
}

/// Parallel sorted arrays node: `keys[i]` is the edge label to `kids[i]`.
/// Capacity grows through [`FANOUT_STAGES`] exactly like an ART's node
/// kinds, just without the fixed-size unions.
struct Node {
    keys: Vec<u8>,
    kids: Vec<Node>,
    /// Present once every key byte has been consumed: the entity's entries,
    /// ascending by `timestamp_ns`.
    entries: Option<Vec<SparseIndexEntry>>,
    /// Current fanout stage capacity (`keys`/`kids` reserve headroom).
    capacity: usize,
}

impl Node {
    fn new() -> Self {
        Self {
            keys: Vec::new(),
            kids: Vec::new(),
            entries: None,
            capacity: FANOUT_STAGES[0],
        }
    }

    /// Grows the fanout to the next stage when the current one is full.
    #[inline]
    fn ensure_room(&mut self) {
        if self.keys.len() < self.capacity {
            return;
        }
        self.capacity = FANOUT_STAGES
            .iter()
            .copied()
            .find(|&c| c > self.capacity)
            .unwrap_or(FANOUT_STAGES[3]);
        self.keys.reserve_exact(self.capacity);
        self.kids.reserve_exact(self.capacity);
    }

    #[inline]
    fn byte_of(key: u128, level: usize) -> u8 {
        debug_assert!(level < KEY_BYTES);
        (key >> (8 * (KEY_BYTES - 1 - level))) as u8
    }

    /// Descends on `key`'s byte at `level`, inserting a fresh node when the
    /// edge label is absent. Returns whether a terminal list was created.
    fn insert(&mut self, key: u128, level: usize, entry: SparseIndexEntry) -> bool {
        if level == KEY_BYTES {
            match &mut self.entries {
                Some(v) => {
                    debug_assert!(
                        v.last()
                            .map_or(true, |e| e.timestamp_ns <= entry.timestamp_ns),
                        "index entries must be inserted in ascending timestamp order"
                    );
                    v.push(entry);
                    false
                }
                None => {
                    self.entries = Some(vec![entry]);
                    true
                }
            }
        } else {
            let b = Self::byte_of(key, level);
            match self.keys.binary_search(&b) {
                Ok(i) => self.kids[i].insert(key, level + 1, entry),
                Err(i) => {
                    self.ensure_room();
                    self.keys.insert(i, b);
                    self.kids.insert(i, Node::new());
                    self.kids[i].insert(key, level + 1, entry)
                }
            }
        }
    }

    fn lookup(&self, key: u128, level: usize) -> Option<&[SparseIndexEntry]> {
        if level == KEY_BYTES {
            return self.entries.as_deref();
        }
        let b = Self::byte_of(key, level);
        match self.keys.binary_search(&b) {
            Ok(i) => self.kids[i].lookup(key, level + 1),
            Err(_) => None,
        }
    }
}

/// Concurrent sparse index: `entity_id -> [SparseIndexEntry]`.
pub struct EntityIndex {
    shards: Box<[RwLock<Option<Node>>]>,
    /// Distinct entities holding at least one entry.
    entities: AtomicU64,
    /// Total entries across all entities.
    entries: AtomicU64,
}

impl EntityIndex {
    pub fn new() -> Self {
        let shards = (0..SHARDS)
            .map(|_| RwLock::new(None))
            .collect::<Vec<_>>()
            .into_boxed_slice();
        Self {
            shards,
            entities: AtomicU64::new(0),
            entries: AtomicU64::new(0),
        }
    }

    #[inline]
    fn shard_of(entity_id: u128) -> usize {
        (entity_id >> (8 * (KEY_BYTES - 1))) as usize
    }

    /// Appends `entry` to `entity_id`'s list. Entries must arrive in
    /// ascending timestamp order (the engine's monotonic clock guarantees
    /// it); the writer thread is the only caller in production.
    pub fn insert(&self, entity_id: u128, entry: SparseIndexEntry) {
        let shard = Self::shard_of(entity_id);
        let mut guard = self.shards[shard].write();
        let root = guard.get_or_insert_with(Node::new);
        if root.insert(entity_id, 1, entry) {
            self.entities.fetch_add(1, Ordering::Relaxed);
        }
        self.entries.fetch_add(1, Ordering::Relaxed);
    }

    /// Runs `f` over `entity_id`'s entries (ascending by timestamp) while
    /// holding the shard's read lock, or over `None` when the entity is
    /// unknown. Keeping the lock inside the closure keeps entry references
    /// from escaping the critical section.
    pub fn lookup<R>(
        &self,
        entity_id: u128,
        f: impl FnOnce(Option<&[SparseIndexEntry]>) -> R,
    ) -> R {
        let shard = Self::shard_of(entity_id);
        let guard = self.shards[shard].read();
        match guard.as_ref() {
            Some(root) => f(root.lookup(entity_id, 1)),
            None => f(None),
        }
    }

    /// Distinct indexed entities.
    pub fn entity_count(&self) -> u64 {
        self.entities.load(Ordering::Relaxed)
    }

    /// Total indexed entries.
    pub fn entry_count(&self) -> u64 {
        self.entries.load(Ordering::Relaxed)
    }

    pub fn is_empty(&self) -> bool {
        self.entry_count() == 0
    }
}

impl Default for EntityIndex {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Debug for EntityIndex {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EntityIndex")
            .field("entities", &self.entity_count())
            .field("entries", &self.entry_count())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn entry(ts: u64, lsn: u64, offset: u64) -> SparseIndexEntry {
        SparseIndexEntry {
            timestamp_ns: ts,
            lsn,
            file_offset: offset,
        }
    }

    /// Deterministic 128-bit pseudo-random keys.
    fn key(i: u64) -> u128 {
        let mut x = i.wrapping_mul(0x9E37_79B9_7F4A_7C15) ^ 0xD6E8_FEB8_6659_FD93;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        let y = x.wrapping_mul(0xFF51_AFD7_ED55_8CCD).rotate_left(31);
        ((x as u128) << 64) | y as u128
    }

    #[test]
    fn insert_lookup_roundtrip() {
        let idx = EntityIndex::new();
        assert!(idx.is_empty());
        for i in 0..512u64 {
            idx.insert(key(i), entry(1000 + i, i + 1, i * 4096));
        }
        assert_eq!(idx.entity_count(), 512);
        assert_eq!(idx.entry_count(), 512);
        for i in 0..512u64 {
            let found = idx.lookup(key(i), |e| e.map(|e| e.to_vec()));
            assert_eq!(found, Some(vec![entry(1000 + i, i + 1, i * 4096)]));
        }
        // Unknown keys are cleanly absent.
        assert_eq!(idx.lookup(key(9999), |e| e.map(<[_]>::len)), None);
    }

    #[test]
    fn entries_append_in_insertion_order() {
        let idx = EntityIndex::new();
        let k = key(7);
        for i in 0..5u64 {
            idx.insert(k, entry(100 * (i + 1), i + 1, 0));
        }
        let ts = idx.lookup(k, |e| {
            e.unwrap()
                .iter()
                .map(|x| x.timestamp_ns)
                .collect::<Vec<_>>()
        });
        assert_eq!(ts, vec![100, 200, 300, 400, 500]);
        assert_eq!(idx.entity_count(), 1);
        assert_eq!(idx.entry_count(), 5);
    }

    /// One node accumulates all 256 possible byte labels; its capacity must
    /// walk the ART stages 4 → 16 → 48 → 256.
    #[test]
    fn fanout_grows_through_stages() {
        let idx = EntityIndex::new();
        let base: u128 = 0x0100_0000_0000_0000_0000_0000_0000_0000;
        for last in 0..=255u64 {
            idx.insert(base | last as u128, entry(last, last + 1, 0));
        }
        assert_eq!(idx.entity_count(), 256);
        // Walk the single shard to find the deepest node's capacity.
        let max_capacity = {
            let mut guard = idx.shards[EntityIndex::shard_of(base)].write();
            let root = guard.as_mut().unwrap();
            fn max_capacity(node: &Node) -> usize {
                node.kids
                    .iter()
                    .map(max_capacity)
                    .max()
                    .unwrap_or(node.capacity)
                    .max(node.capacity)
            }
            max_capacity(root)
        };
        assert_eq!(max_capacity, 256);
    }

    #[test]
    fn shared_prefixes_do_not_collide() {
        let idx = EntityIndex::new();
        let a: u128 = 0xABCD_0000_0000_0000_0000_0000_0000_00F1;
        let b: u128 = 0xABCD_0000_0000_0000_0000_0000_0000_00F2;
        let c: u128 = 0xABCE_0000_0000_0000_0000_0000_0000_00F1;
        idx.insert(a, entry(1, 1, 10));
        idx.insert(b, entry(1, 2, 20));
        idx.insert(c, entry(1, 3, 30));
        assert_eq!(idx.lookup(a, |e| e.unwrap()[0].file_offset), 10);
        assert_eq!(idx.lookup(b, |e| e.unwrap()[0].file_offset), 20);
        assert_eq!(idx.lookup(c, |e| e.unwrap()[0].file_offset), 30);
        assert_eq!(idx.entity_count(), 3);
    }

    #[test]
    fn shards_are_independent() {
        let idx = EntityIndex::new();
        let k0: u128 = 0x00FF_FFFF_FFFF_FFFF_FFFF_FFFF_FFFF_FFFF;
        let k1: u128 = 0xFF00_0000_0000_0000_0000_0000_0000_0000;
        assert_ne!(EntityIndex::shard_of(k0), EntityIndex::shard_of(k1));
        idx.insert(k0, entry(5, 1, 1));
        idx.insert(k1, entry(5, 2, 2));
        assert_eq!(idx.lookup(k0, |e| e.unwrap()[0].lsn), 1);
        assert_eq!(idx.lookup(k1, |e| e.unwrap()[0].lsn), 2);
    }

    #[test]
    fn concurrent_writers_and_readers() {
        let idx = EntityIndex::new();
        const WRITERS: u64 = 4;
        const PER_WRITER: u64 = 500;
        std::thread::scope(|s| {
            for w in 0..WRITERS {
                let idx = &idx;
                s.spawn(move || {
                    for i in 0..PER_WRITER {
                        let k = key(w * PER_WRITER + i);
                        idx.insert(k, entry(i + 1, i + 1, i));
                        // Readers must never observe a torn list.
                        idx.lookup(k, |e| {
                            let es = e.expect("just inserted");
                            assert!(es.last().is_some_and(|x| x.timestamp_ns == i + 1));
                        });
                    }
                });
            }
            for _ in 0..4 {
                let idx = &idx;
                s.spawn(move || {
                    for i in 0..2000u64 {
                        let k = key(i % (WRITERS * PER_WRITER));
                        idx.lookup(k, |e| {
                            if let Some(es) = e {
                                assert!(es
                                    .windows(2)
                                    .all(|w| w[0].timestamp_ns <= w[1].timestamp_ns));
                            }
                        });
                    }
                });
            }
        });
        assert_eq!(idx.entity_count(), WRITERS * PER_WRITER);
        assert_eq!(idx.entry_count(), WRITERS * PER_WRITER);
    }
}
