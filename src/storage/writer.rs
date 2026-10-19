//! The append-only write path: packs variable-length records into
//! 4 KiB pages, advances the globally contiguous LSN, batches complete pages
//! into single positioned writes and drives the `fdatasync` flush protocol.
//!
//! Page packing rules (locked decisions + SPEC Phase 2):
//! * records either fit whole inside a page's free space, or they span page
//!   boundaries using continuation pages (`REC_FLAG_CONTINUES_ON_NEXT_PAGE`
//!   on the record header, `PAGE_FLAG_STARTS_WITH_CONTINUATION` on every
//!   continuation page);
//! * a spanning record always starts at byte `PAGE_HEADER_SIZE` of a fresh
//!   page and fills exactly `PAGE_PAYLOAD_SIZE - RECORD_HEADER_SIZE` payload
//!   bytes, then exactly `PAGE_PAYLOAD_SIZE` per continuation page;
//! * every page that reaches the disk is final: `PAGE_FLAG_CLOSED`, and its
//!   payload tail is zero filled;
//! * a page's `prev_page_lsn` is the highest LSN touching the *previous*
//!   page (0 for page 1), enforced again by the scanner.

use crate::error::{ChronoError, Result};
use crate::storage::file::{AlignedBuf, DbFile};
use crate::storage::page::{
    page_offset, stamp_page_crc, PageHeader, PAGE_FLAG_CLOSED, PAGE_FLAG_STARTS_WITH_CONTINUATION,
    PAGE_HEADER_SIZE, PAGE_PAYLOAD_SIZE, PAGE_SIZE,
};
use crate::storage::record::{
    RecordHeader, RECORD_HEADER_SIZE, REC_FLAG_CONTINUES_ON_NEXT_PAGE, REC_FLAG_TOMBSTONE,
};

/// Writer tunables.
#[derive(Clone, Copy, Debug)]
pub struct WriterOptions {
    /// Hard cap on payload bytes per record; larger payloads are rejected
    /// with [`ChronoError::RecordTooLarge`].
    pub max_record_payload: usize,
    /// How many complete pages accumulate before the single positioned
    /// write (syscall batching).
    pub write_batch_pages: usize,
}

impl Default for WriterOptions {
    fn default() -> Self {
        Self {
            max_record_payload: 1 << 20, // 1 MiB
            write_batch_pages: 32,       // 128 KiB per write
        }
    }
}

/// A record staged for the packer. The payload borrows from the caller's
/// buffer, so packing into a page costs no allocation.
#[derive(Clone, Copy, Debug)]
pub struct StagedRecord<'a> {
    pub entity_id: u128,
    pub timestamp_ns: u64,
    pub payload: &'a [u8],
    /// Payload schema version (SPEC §2.4).
    pub schema_version: u16,
    /// Record flags (tombstone bit, etc.).
    pub flags: u16,
}

impl<'a> StagedRecord<'a> {
    pub fn new(entity_id: u128, timestamp_ns: u64, payload: &'a [u8]) -> Self {
        Self {
            entity_id,
            timestamp_ns,
            payload,
            schema_version: 0,
            flags: 0,
        }
    }

    pub fn tombstone(entity_id: u128, timestamp_ns: u64) -> Self {
        let mut r = Self::new(entity_id, timestamp_ns, &[]);
        r.flags = REC_FLAG_TOMBSTONE;
        r
    }
}

/// Payload budget for the first (header-carrying) fragment of a fresh page.
pub const FIRST_FRAGMENT_MAX_PAYLOAD: usize = PAGE_PAYLOAD_SIZE - RECORD_HEADER_SIZE;

/// Upper bound of the file bytes a record with at most `max_payload`
/// payload bytes can occupy, page headers and page-boundary layout
/// included. Used by the mmap manager to size its region overlap so a
/// record can never straddle two mapping regions.
pub const fn max_record_extent(max_payload: usize) -> u64 {
    if max_payload + RECORD_HEADER_SIZE <= PAGE_PAYLOAD_SIZE {
        // Never splits: the record sits inside one page.
        return PAGE_SIZE as u64;
    }
    let rest = max_payload - FIRST_FRAGMENT_MAX_PAYLOAD;
    let continuation_pages = rest.div_ceil(PAGE_PAYLOAD_SIZE) as u64;
    (1 + continuation_pages) * PAGE_SIZE as u64
}

struct OpenPage {
    /// Payload bytes occupied counting from `PAGE_HEADER_SIZE`.
    used: usize,
    fragments: u16,
    flags: u16,
    max_lsn: u64,
}

impl OpenPage {
    fn fresh() -> Self {
        Self {
            used: 0,
            fragments: 0,
            flags: 0,
            max_lsn: 0,
        }
    }
}

/// Where an appended record landed on disk.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct AppendOutcome {
    /// The record's assigned LSN.
    pub lsn: u64,
    /// Absolute byte offset of the record header (inside its page).
    pub file_offset: u64,
}

/// State recovered from a validated scan of an existing file, used to
/// resume the packer ([`PageWriter::resume_for`]).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct WriterResume {
    /// Highest valid LSN already in the file (`0` when nothing was found).
    pub last_lsn: u64,
    /// Page index of the first page not holding recovered data — where the
    /// next sealed page lands.
    pub next_page: u64,
    /// Highest LSN touching the last recovered page: the `prev_page_lsn`
    /// contract of the next page to be sealed.
    pub prev_page_max_lsn: u64,
}

/// Packs records into pages and writes them to the database file.
pub struct PageWriter {
    db: DbFile,
    opts: WriterOptions,
    /// LSN the next record will receive; strictly contiguous from 1.
    next_lsn: u64,
    /// Page index of the currently open in-memory page.
    next_page: u64,
    /// Highest LSN of the most recently closed page — the `prev_page_lsn`
    /// contract of the page currently being built. `0` initially.
    prev_page_max_lsn: u64,
    open: OpenPage,
    page: AlignedBuf,
    /// Staging area for `write_batch_pages` complete pages.
    out: AlignedBuf,
    out_pages: usize,
    /// First page index not yet written to the file.
    first_unwritten_page: u64,
    /// Highest LSN whose page(s) reached the file (not just `out`).
    last_flushed_lsn: u64,
    /// Highest LSN that survived a completed `sync()` (fdatasync applied).
    last_durable_lsn: u64,
}

impl PageWriter {
    /// Attaches to a freshly created database file (superblock only).
    pub fn create_for(db: DbFile, opts: WriterOptions) -> Result<Self> {
        let out_len = opts.write_batch_pages * PAGE_SIZE;
        let out = AlignedBuf::zeroed(out_len, PAGE_SIZE)?;
        Ok(Self {
            db,
            opts,
            next_lsn: 1,
            next_page: 1,
            prev_page_max_lsn: 0,
            open: OpenPage::fresh(),
            page: AlignedBuf::zeroed(PAGE_SIZE, PAGE_SIZE)?,
            out,
            out_pages: 0,
            first_unwritten_page: 1,
            last_flushed_lsn: 0,
            last_durable_lsn: 0,
        })
    }

    /// Attaches to an existing database file after recovery: LSN assignment
    /// continues from `resume.last_lsn`, the next sealed page lands at
    /// `resume.next_page`, and everything recovered counts as flushed and
    /// durable (it survived a completed `fdatasync` by construction — a
    /// torn page is cut away by the recovery scan instead).
    pub fn resume_for(db: DbFile, opts: WriterOptions, resume: WriterResume) -> Result<Self> {
        let out_len = opts.write_batch_pages * PAGE_SIZE;
        let out = AlignedBuf::zeroed(out_len, PAGE_SIZE)?;
        let next_page = resume.next_page.max(1);
        Ok(Self {
            db,
            opts,
            next_lsn: resume.last_lsn + 1,
            next_page,
            prev_page_max_lsn: resume.prev_page_max_lsn,
            open: OpenPage::fresh(),
            page: AlignedBuf::zeroed(PAGE_SIZE, PAGE_SIZE)?,
            out,
            out_pages: 0,
            first_unwritten_page: next_page,
            last_flushed_lsn: resume.last_lsn,
            last_durable_lsn: resume.last_lsn,
        })
    }

    // -- introspection ------------------------------------------------------

    #[inline]
    pub fn next_lsn(&self) -> u64 {
        self.next_lsn
    }
    /// Highest assigned LSN (0 for an empty engine).
    #[inline]
    pub fn last_assigned_lsn(&self) -> u64 {
        self.next_lsn - 1
    }
    /// Highest LSN whose pages were written to disk (without fsync).
    #[inline]
    pub fn written_lsn(&self) -> u64 {
        self.last_flushed_lsn.max(self.last_durable_lsn)
    }
    /// Highest LSN that survived `fdatasync`.
    #[inline]
    pub fn durable_lsn(&self) -> u64 {
        self.last_durable_lsn
    }
    /// Data pages flushed so far (open page excluded).
    #[inline]
    pub fn flushed_pages(&self) -> u64 {
        self.first_unwritten_page - 1
    }
    /// Page index the packer will fill next (= flushed pages + 1).
    #[inline]
    pub fn next_page(&self) -> u64 {
        self.next_page
    }
    #[inline]
    pub fn options(&self) -> &WriterOptions {
        &self.opts
    }
    pub fn file_path(&self) -> &std::path::Path {
        self.db.path()
    }
    pub fn is_read_only_file(&self) -> bool {
        self.db.is_read_only()
    }

    // -- the packer ----------------------------------------------------------

    /// Packs one variable-length record; returns its assigned LSN.
    pub fn append(&mut self, rec: StagedRecord) -> Result<u64> {
        self.append_located(rec).map(|outcome| outcome.lsn)
    }

    /// Packs one variable-length record and reports where it landed — LSN
    /// plus the absolute file offset of its header — so the read path can
    /// index the record.
    pub fn append_located(&mut self, rec: StagedRecord) -> Result<AppendOutcome> {
        let payload_len = rec.payload.len();
        if payload_len > self.opts.max_record_payload {
            return Err(ChronoError::RecordTooLarge {
                size: payload_len,
                max: self.opts.max_record_payload,
            });
        }
        let lsn = self.next_lsn;
        debug_assert_eq!(
            lsn, self.next_lsn,
            "LSN assignment must be monotonically one-at-a-time"
        );

        // Encode the header once with its final flags, checksum once.
        let first_frag = FIRST_FRAGMENT_MAX_PAYLOAD.min(payload_len);
        let continues = payload_len > first_frag;
        let mut header = RecordHeader::new(lsn, rec.timestamp_ns, rec.entity_id, payload_len);
        header.schema_version = rec.schema_version;
        header.flags = rec.flags;
        if continues {
            header.flags |= REC_FLAG_CONTINUES_ON_NEXT_PAGE;
        }
        header.record_crc = header.compute_self_crc(rec.payload);
        let mut hdr_bytes = [0u8; RECORD_HEADER_SIZE];
        header.encode(&mut hdr_bytes);

        let file_offset;
        if RECORD_HEADER_SIZE + payload_len <= PAGE_PAYLOAD_SIZE {
            // Normal record; may need a fresh page, but never splits.
            let need = RECORD_HEADER_SIZE + payload_len;
            let free = PAGE_PAYLOAD_SIZE - self.open.used;
            if free < need {
                self.close_current_page()?; // leaves the page buffer fresh
            }
            file_offset = page_offset(self.next_page) + (PAGE_HEADER_SIZE + self.open.used) as u64;
            self.write_framed_fragment(&hdr_bytes, rec.payload)?;
            self.open.expose(lsn);
        } else {
            // Spanning record: always starts precisely at a fresh page.
            if self.open.used > 0 {
                self.close_current_page()?; // leaves the page buffer fresh
            }
            file_offset = page_offset(self.next_page) + PAGE_HEADER_SIZE as u64;
            self.write_framed_fragment(&hdr_bytes, &rec.payload[..first_frag])?;
            self.open.expose(lsn);
            let mut written = first_frag;
            if continues {
                self.close_current_page()?;
                while written < payload_len {
                    self.open.mark_for_continuation();
                    let take = (PAGE_PAYLOAD_SIZE).min(payload_len - written);
                    self.write_raw_fragment(&rec.payload[written..written + take])?;
                    written += take;
                    self.open.expose(lsn);
                    if written < payload_len {
                        self.close_current_page()?;
                    }
                }
            }
        }
        self.next_lsn += 1;
        Ok(AppendOutcome { lsn, file_offset })
    }

    /// Record-with-header fragment (`hdr_bytes` + `payload`) in the open page.
    fn write_framed_fragment(
        &mut self,
        hdr_bytes: &[u8; RECORD_HEADER_SIZE],
        payload: &[u8],
    ) -> Result<()> {
        let at = PAGE_HEADER_SIZE + self.open.used;
        let page = self.page.as_mut_slice();
        page[at..at + RECORD_HEADER_SIZE].copy_from_slice(hdr_bytes);
        page[at + RECORD_HEADER_SIZE..at + RECORD_HEADER_SIZE + payload.len()]
            .copy_from_slice(payload);
        self.open.used += RECORD_HEADER_SIZE + payload.len();
        self.open.fragments += 1;
        Ok(())
    }

    /// Raw continuation fragment: bare payload continuing a record.
    fn write_raw_fragment(&mut self, chunk: &[u8]) -> Result<()> {
        let page = self.page.as_mut_slice();
        let at = PAGE_HEADER_SIZE + self.open.used;
        page[at..at + chunk.len()].copy_from_slice(chunk);
        self.open.used += chunk.len();
        self.open.fragments += 1;
        Ok(())
    }

    /// Zero-fill a brand-new page and reset its bookkeeping.
    pub fn open_fresh_page(&mut self) {
        self.page.as_mut_slice().fill(0);
        self.open = OpenPage::fresh();
    }

    /// Terminate the open page: zero-pad the payload tail, write the 32-byte
    /// header, stamp the CRC32-C, and stage the page for the positioned
    /// write. Also re-applies the `prev_page_lsn` contract.
    fn close_current_page(&mut self) -> Result<()> {
        {
            let page = self.page.as_mut_slice();
            page[PAGE_HEADER_SIZE + self.open.used..PAGE_SIZE].fill(0);
            let header = PageHeader {
                page_epoch: self.next_page,
                prev_page_lsn: self.prev_page_max_lsn,
                record_count: self.open.fragments,
                flags: self.open.flags | PAGE_FLAG_CLOSED,
            };
            header.write(page.as_mut().try_into().expect("buffer is PAGE_SIZE"));
            stamp_page_crc(page.as_mut().try_into().expect("buffer is PAGE_SIZE"));
        }
        let slot = self.out_pages * PAGE_SIZE;
        let (out, page) = (&mut self.out, &self.page);
        out.as_mut_slice()[slot..slot + PAGE_SIZE].copy_from_slice(page.as_slice());
        self.out_pages += 1;

        self.prev_page_max_lsn = self.open.max_lsn;
        self.next_page += 1;
        // The sealed page left the packer: zero the buffer and start fresh so
        // nothing packed later can contaminate the sealed image (this is what
        // makes a `sync()`-triggered seal and a full-page seal equivalent).
        self.page.as_mut_slice().fill(0);
        self.open = OpenPage::fresh();
        if self.out_pages >= self.opts.write_batch_pages {
            self.flush_staging()?;
        }
        Ok(())
    }

    /// One positioned write over every staged page.
    fn flush_staging(&mut self) -> Result<()> {
        if self.out_pages == 0 {
            return Ok(());
        }
        self.db
            .write_pages(self.first_unwritten_page, &self.out, self.out_pages)?;
        self.first_unwritten_page += self.out_pages as u64;
        self.last_flushed_lsn = self.prev_page_max_lsn.max(self.last_flushed_lsn);
        self.out_pages = 0;
        Ok(())
    }

    /// `sync_data()` flush protocol: close+write any open page, write
    /// staged pages, `fdatasync(2)` the file. Returns the highest LSN now
    /// durable.
    pub fn sync(&mut self) -> Result<u64> {
        if self.open.used > 0 {
            self.close_current_page()?;
        }
        self.flush_staging()?;
        self.db.sync_data()?;
        self.last_durable_lsn = self.last_flushed_lsn;
        Ok(self.last_durable_lsn)
    }

    /// Pages to the file without fsync — page-cache visibility.
    pub fn flush(&mut self) -> Result<u64> {
        if self.open.used > 0 {
            self.close_current_page()?;
        }
        self.flush_staging()?;
        Ok(self.last_flushed_lsn)
    }

    /// Writable handle view for higher layers.
    pub fn file(&self) -> &DbFile {
        &self.db
    }
}

impl OpenPage {
    /// Records the LSN currently touching this page.
    #[inline]
    fn expose(&mut self, lsn: u64) {
        self.max_lsn = lsn;
    }

    /// Records that the following fragment continues a record from the
    /// previous page.
    #[inline]
    fn mark_for_continuation(&mut self) {
        self.flags |= PAGE_FLAG_STARTS_WITH_CONTINUATION;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn max_record_extent_covers_single_page_and_spanning_records() {
        // Everything up to a full single page stays inside that page.
        assert_eq!(max_record_extent(0), PAGE_SIZE as u64);
        assert_eq!(
            max_record_extent(FIRST_FRAGMENT_MAX_PAYLOAD),
            PAGE_SIZE as u64
        );
        // One payload byte more starts a continuation page.
        assert_eq!(
            max_record_extent(FIRST_FRAGMENT_MAX_PAYLOAD + 1),
            2 * PAGE_SIZE as u64
        );
        // Exact spanning layout: 1 MiB spans 1 + ceil(1044556 / 4064) = 259
        // pages once page headers are counted.
        assert_eq!(max_record_extent(1 << 20), 259 * PAGE_SIZE as u64);
        // The bound dominates a maximal record's file extent with room to
        // spare: first-page tail (4064) + 257 full middle pages + at most a
        // full last page (4064) = 1,060,800 ≤ 1,060,864.
        assert!((4064 + 257 * 4096 + 4064) as u64 <= max_record_extent(1 << 20));
    }
}
