//! The read path facade (SPEC §3.1/§4): as-of resolution, latest-record
//! lookup and history scans over the mmap + sparse index.
//!
//! Visibility contract: a record is observable once its bytes reached the
//! file (the worker refreshes the mapping and indexes it only after the
//! durability step) **and** its LSN is ≤ the published `committed_lsn`;
//! queries re-check the LSN gate so a reader that races the end of a batch
//! sees the previous, fully committed snapshot instead of a torn one.
//!
//! Payload ownership: a record that fits in one page is returned as a slice
//! pointing straight into the mapping — zero copies. A record that spans
//! pages has its payload *interrupted by page headers* on disk, so it
//! cannot be one contiguous slice; those are reassembled into an owned
//! buffer ([`ReadRecord::payload`] exposes both variants as `&[u8]`).
//! Every payload is CRC-verified against its record header on the way out.

use std::borrow::Cow;

use crate::error::{ChronoError, Result};
use crate::index::SparseIndexEntry;
use crate::storage::page::{page_of, page_offset, PAGE_HEADER_SIZE, PAGE_PAYLOAD_SIZE};
use crate::storage::record::{
    RecordHeader, RecordRef, RECORD_HEADER_SIZE, REC_FLAG_CONTINUES_ON_NEXT_PAGE, RH_OFF_RECORD_CRC,
};

use super::Engine;

/// A record read through the engine's mapping.
#[derive(Clone, Debug, PartialEq)]
pub struct ReadRecord<'a> {
    pub header: RecordHeader,
    /// `Borrowed` slices the mapping (zero-copy); `Owned` holds the
    /// reassembly buffer of a multi-page record.
    pub payload: Cow<'a, [u8]>,
}

impl ReadRecord<'_> {
    /// The payload bytes, regardless of ownership.
    pub fn payload(&self) -> &[u8] {
        self.payload.as_ref()
    }

    /// Borrows this record as a plain [`RecordRef`].
    pub fn as_record_ref(&self) -> RecordRef<'_> {
        RecordRef {
            header: self.header,
            payload: self.payload.as_ref(),
        }
    }

    pub fn lsn(&self) -> u64 {
        self.header.lsn
    }

    pub fn entity_id(&self) -> u128 {
        self.header.entity_id
    }

    pub fn timestamp_ns(&self) -> u64 {
        self.header.timestamp_ns
    }

    pub fn is_tombstone(&self) -> bool {
        self.header.is_tombstone()
    }
}

/// Iterator over one entity's history, ascending by timestamp. Yields
/// [`ReadRecord`]s borrowed from (or reassembled out of) the engine's
/// mapping; tombstone records are part of the raw history.
pub struct HistoryIter<'e> {
    engine: &'e Engine,
    entries: std::vec::IntoIter<SparseIndexEntry>,
}

impl HistoryIter<'_> {
    /// Records left to yield.
    pub fn remaining(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.len() == 0
    }
}

impl<'e> Iterator for HistoryIter<'e> {
    type Item = Result<ReadRecord<'e>>;

    fn next(&mut self) -> Option<Self::Item> {
        self.entries.next().map(|e| self.engine.read_indexed(e))
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.entries.size_hint()
    }
}

impl ExactSizeIterator for HistoryIter<'_> {}

/// Verifies a record checksum from its on-disk header image and payload.
fn verify_crc(header: &RecordHeader, header_image: &[u8], payload: &[u8]) -> Result<()> {
    let mut image = [0u8; RECORD_HEADER_SIZE];
    image.copy_from_slice(&header_image[..RECORD_HEADER_SIZE]);
    image[RH_OFF_RECORD_CRC..RECORD_HEADER_SIZE].fill(0);
    if RecordHeader::compute_record_crc(&image, payload) != header.record_crc {
        return Err(ChronoError::RecordChecksumMismatch { lsn: header.lsn });
    }
    Ok(())
}

impl Engine {
    /// Reads the record at `entry` from the mapping and verifies its CRC.
    fn read_indexed(&self, entry: SparseIndexEntry) -> Result<ReadRecord<'_>> {
        let unmapped = |what: &str| ChronoError::CorruptRecord {
            offset: entry.file_offset,
            reason: format!("{what} lies outside the mapped, written file prefix"),
        };
        let head = self
            .mmap
            .slice(entry.file_offset, RECORD_HEADER_SIZE)
            .ok_or_else(|| unmapped("record header"))?;
        let header = RecordHeader::decode(head)?;
        let payload_len = header.payload_size();

        if header.flags & REC_FLAG_CONTINUES_ON_NEXT_PAGE == 0 {
            // Single-fragment record: header and payload are contiguous in
            // its page, so the payload is a zero-copy mapping slice.
            let raw = self
                .mmap
                .slice(entry.file_offset, RECORD_HEADER_SIZE + payload_len)
                .ok_or_else(|| unmapped("record payload"))?;
            verify_crc(
                &header,
                &raw[..RECORD_HEADER_SIZE],
                &raw[RECORD_HEADER_SIZE..],
            )?;
            Ok(ReadRecord {
                header,
                payload: Cow::Borrowed(&raw[RECORD_HEADER_SIZE..]),
            })
        } else {
            // Spanning record: the first fragment fills its page up to the
            // page end (`RECORD_HEADER_SIZE + first fragment ==
            // PAGE_PAYLOAD_SIZE`, guaranteed by the writer); each
            // continuation page then carries `PAGE_PAYLOAD_SIZE` raw bytes
            // after its own page header.
            let first = self
                .mmap
                .slice(entry.file_offset, PAGE_PAYLOAD_SIZE)
                .ok_or_else(|| unmapped("first fragment"))?;
            let mut assembled = Vec::with_capacity(payload_len);
            let first_payload = PAGE_PAYLOAD_SIZE - RECORD_HEADER_SIZE;
            let take = first_payload.min(payload_len);
            assembled.extend_from_slice(&first[RECORD_HEADER_SIZE..RECORD_HEADER_SIZE + take]);

            let mut remaining = payload_len - take;
            let mut page = page_of(entry.file_offset) + 1;
            while remaining > 0 {
                let take = PAGE_PAYLOAD_SIZE.min(remaining);
                let chunk = self
                    .mmap
                    .slice(page_offset(page) + PAGE_HEADER_SIZE as u64, take)
                    .ok_or_else(|| unmapped("continuation fragment"))?;
                assembled.extend_from_slice(chunk);
                remaining -= take;
                page += 1;
            }

            verify_crc(&header, &first[..RECORD_HEADER_SIZE], &assembled)?;
            Ok(ReadRecord {
                header,
                payload: Cow::Owned(assembled),
            })
        }
    }

    /// Resolves the entity's state as of `timestamp_ns` (SPEC §4.2): binary
    /// searches the sparse index for the latest committed record with
    /// `record.timestamp_ns <= timestamp_ns`, dereferences it and verifies
    /// its checksum. Tombstones resolve to `None`.
    pub fn get_as_of(&self, entity_id: u128, timestamp_ns: u64) -> Result<Option<ReadRecord<'_>>> {
        let committed = self.chain.committed_lsn();
        let hit = self.index.lookup(entity_id, |entries| {
            let entries = entries?;
            // Entries enter the index in LSN order, so the committed ones
            // form a prefix; anything after it is already indexed but not
            // yet published and must not hide older committed versions.
            let visible = &entries[..entries.partition_point(|e| e.lsn <= committed)];
            let pos = visible.partition_point(|e| e.timestamp_ns <= timestamp_ns);
            (pos > 0).then(|| visible[pos - 1])
        });
        match hit {
            None => Ok(None),
            Some(entry) => {
                let record = self.read_indexed(entry)?;
                if record.header.is_tombstone() {
                    Ok(None)
                } else {
                    Ok(Some(record))
                }
            }
        }
    }

    /// Most recent committed record of the entity — tombstones included, so
    /// callers can distinguish "never seen" (`None`) from "deleted".
    pub fn get_latest(&self, entity_id: u128) -> Result<Option<ReadRecord<'_>>> {
        let committed = self.chain.committed_lsn();
        let hit = self.index.lookup(entity_id, |entries| {
            let entries = entries?;
            let visible = &entries[..entries.partition_point(|e| e.lsn <= committed)];
            visible.last().copied()
        });
        hit.map(|entry| self.read_indexed(entry)).transpose()
    }

    /// Streams every committed record of the entity with
    /// `from_ns <= timestamp_ns <= to_ns` in ascending time order. The
    /// index snapshot is taken at call time; payloads are dereferenced
    /// lazily.
    pub fn scan_history(
        &self,
        entity_id: u128,
        from_ns: u64,
        to_ns: u64,
    ) -> Result<HistoryIter<'_>> {
        let committed = self.chain.committed_lsn();
        let entries: Vec<SparseIndexEntry> =
            self.index.lookup(entity_id, |entries| match entries {
                None => Vec::new(),
                Some(es) => es
                    .iter()
                    .filter(|e| e.lsn <= committed && (from_ns..=to_ns).contains(&e.timestamp_ns))
                    .copied()
                    .collect(),
            });
        Ok(HistoryIter {
            engine: self,
            entries: entries.into_iter(),
        })
    }
}

/// The engine contract of SPEC §4.1. [`Engine`] implements every method
/// natively; the trait lets callers program against the interface.
pub trait ChronoEngine {
    fn append(&self, entity_id: u128, payload: &[u8]) -> Result<u64>;
    fn append_batch(&self, events: &[(u128, &[u8])]) -> Result<Vec<u64>>;
    fn get_as_of(&self, entity_id: u128, timestamp_ns: u64) -> Result<Option<ReadRecord<'_>>>;
    fn scan_history(&self, entity_id: u128, from_ns: u64, to_ns: u64) -> Result<HistoryIter<'_>>;
    fn get_latest(&self, entity_id: u128) -> Result<Option<ReadRecord<'_>>>;
    fn sync(&self) -> Result<u64>;
}

impl ChronoEngine for Engine {
    fn append(&self, entity_id: u128, payload: &[u8]) -> Result<u64> {
        Engine::append(self, entity_id, payload)
    }

    fn append_batch(&self, events: &[(u128, &[u8])]) -> Result<Vec<u64>> {
        Engine::append_batch(self, events)
    }

    fn get_as_of(&self, entity_id: u128, timestamp_ns: u64) -> Result<Option<ReadRecord<'_>>> {
        Engine::get_as_of(self, entity_id, timestamp_ns)
    }

    fn scan_history(&self, entity_id: u128, from_ns: u64, to_ns: u64) -> Result<HistoryIter<'_>> {
        Engine::scan_history(self, entity_id, from_ns, to_ns)
    }

    fn get_latest(&self, entity_id: u128) -> Result<Option<ReadRecord<'_>>> {
        Engine::get_latest(self, entity_id)
    }

    fn sync(&self) -> Result<u64> {
        Engine::sync(self)
    }
}
