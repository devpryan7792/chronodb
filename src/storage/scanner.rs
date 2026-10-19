//! Sequential, write-ahead-log-style scanner over the record pages.
//!
//! Responsibilities (SPEC §5.2 recovery on boot):
//! 1. walk pages `1..` of the file (page 0 is the superblock),
//! 2. verify every page CRC32-C plus the header invariants
//!    (`page_epoch == page index`, `prev_page_lsn` continuity, strictly
//!    contiguous LSNs, non-decreasing timestamps, zero-padded tails,
//!    `record_count` matching),
//! 3. stream records — records fully inside one page are borrowed
//!    **zero-copy** out of the page buffer; a record that spans pages is
//!    reassembled once into a scratch buffer,
//! 4. treat a trailing all-zero page as pre-allocation free space and stop,
//! 5. optionally truncate a corrupt (non-zero, invalid) tail back to the
//!    last valid page boundary with `ftruncate`.
//!
//! Shared by `chronoctl verify/tail`, open-recovery and the test suite.

use crate::error::{ChronoError, Result};
use crate::storage::crc::Crc32cHasher;
use crate::storage::file::{AlignedBuf, DbFile};
use crate::storage::page::{
    parse_page_header, PageHeader, PAGE_FLAG_CLOSED, PAGE_FLAG_STARTS_WITH_CONTINUATION,
    PAGE_HEADER_SIZE, PAGE_SIZE, PAGE_SIZE_U64,
};
use crate::storage::record::{
    record_encoded_len, RecordHeader, RecordRef, RECORD_HEADER_SIZE, RH_OFF_RECORD_CRC,
};

const ZEROS4: [u8; 4] = [0; 4];

/// Per-page facts collected while scanning.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PageInfo {
    pub page_index: u64,
    /// Equals `page_index` in a healthy database (the scanner checks this).
    pub page_epoch: u64,
    pub prev_page_lsn: u64,
    /// Number of record fragments stored in this page.
    pub record_count: u16,
    pub flags: u16,
    /// Payload bytes actually used inside this page, including headers.
    pub used_bytes: usize,
    /// Highest LSN of any record fragment stored here.
    pub max_lsn: u64,
}

impl PageInfo {
    pub fn is_closed(&self) -> bool {
        self.flags & PAGE_FLAG_CLOSED != 0
    }
    pub fn starts_with_continuation(&self) -> bool {
        self.flags & PAGE_FLAG_STARTS_WITH_CONTINUATION != 0
    }
}

/// Zero-copy record view borrowed from the scan buffers.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct RecordView<'a> {
    pub header: RecordHeader,
    pub payload: &'a [u8],
    pub start_page: u64,
    /// Byte offset of the record header inside its starting page.
    pub start_offset: usize,
    /// 1 when the record lives entirely inside one page.
    pub pages_spanned: u64,
}

impl RecordView<'_> {
    #[inline]
    pub fn as_record_ref(&self) -> RecordRef<'_> {
        RecordRef {
            header: self.header,
            payload: self.payload,
        }
    }
    #[inline]
    pub fn lsn(&self) -> u64 {
        self.header.lsn
    }
    #[inline]
    pub fn entity_id(&self) -> u128 {
        self.header.entity_id
    }
    #[inline]
    pub fn timestamp_ns(&self) -> u64 {
        self.header.timestamp_ns
    }
    #[inline]
    pub fn is_tombstone(&self) -> bool {
        self.header.is_tombstone()
    }
}

/// Aggregate outcome of a scan.
#[derive(Debug, Default)]
pub struct ScanReport {
    pub pages: Vec<PageInfo>,
    /// Logical records (a spanning record counts exactly once).
    pub record_count: u64,
    pub min_lsn: Option<u64>,
    pub max_lsn: Option<u64>,
    /// Trailing pages found entirely zero — free pre-allocation space.
    pub free_tail_pages: u64,
    /// Total bytes cut away by tail truncation (torn pages plus a trailing
    /// sub-page remnant), when `truncate_tail` fired.
    pub truncated_bytes: Option<u64>,
}

/// Streaming record visitor. The `RecordView` borrows from the scan
/// buffers, which stay valid for the duration of the callback.
pub type RecordVisitor<'v> = dyn FnMut(RecordView<'_>) -> Result<()> + 'v;

/// Scans the record pages of `db` and calls `visit` for every record.
///
/// * `truncate_tail = false` — a corrupt page aborts with the underlying
///   [`ChronoError`].
/// * `truncate_tail = true`  — a corrupt page truncates the file back to the
///   last valid page boundary (SPEC §5.2 step 3); the scan then ends
///   successfully and [`ScanReport::truncated_bytes`] records the cut.
pub fn for_each_record<F>(db: &DbFile, truncate_tail: bool, visit: F) -> Result<ScanReport>
where
    F: FnMut(RecordView<'_>) -> Result<()>,
{
    let mut file_len = db.len()?;
    // A trailing byte range shorter than a page can only be the residue of a
    // torn extend / pre-allocation; it can never hold a valid page. Repair
    // cuts it before the page walk (strict scans leave it in place — it is
    // unreachable either way, since reads are page-aligned).
    let mut truncated_base = 0u64;
    let partial = file_len % PAGE_SIZE_U64;
    if truncate_tail && partial != 0 {
        file_len -= partial;
        db.truncate_to_len(file_len)?;
        truncated_base = partial;
    }
    let file_pages = file_len / PAGE_SIZE_U64;
    let job = ScanJob {
        db,
        truncate_tail,
        visit,
        file_pages,
        truncated_base,
        page: AlignedBuf::zeroed(PAGE_SIZE, PAGE_SIZE)?,
        span_payload: Vec::new(),
        next_lsn: 1,
        last_ts: 0,
        prev_touched: 0,
        first_completed: None,
        last_completed: None,
        records: 0,
        pending: None,
    };
    job.run()
}

/// Same behaviour as [`for_each_record`]; the recovery-facing alias.
pub fn scan_and_validate<F>(db: &DbFile, truncate_tail: bool, visit: F) -> Result<ScanReport>
where
    F: FnMut(RecordView<'_>) -> Result<()>,
{
    for_each_record(db, truncate_tail, visit)
}

struct Pending {
    header: RecordHeader,
    hasher: Crc32cHasher,
    remaining: u64,
    start_page: u64,
    start_offset: usize,
}

struct ScanJob<'a, F: FnMut(RecordView<'_>) -> Result<()>> {
    db: &'a DbFile,
    truncate_tail: bool,
    visit: F,
    file_pages: u64,
    /// Bytes already cut by the partial-tail repair, carried into the report.
    truncated_base: u64,
    page: AlignedBuf,
    /// Reassembly buffer for records that span pages (empty otherwise).
    span_payload: Vec<u8>,
    next_lsn: u64,
    last_ts: u64,
    prev_touched: u64,
    /// First/last LSN of *completed* records, tracked at the visit sites so
    /// the report never counts an in-flight record's fragment pages.
    first_completed: Option<u64>,
    last_completed: Option<u64>,
    records: u64,
    pending: Option<Pending>,
}

impl<'a, F: FnMut(RecordView<'_>) -> Result<()>> ScanJob<'a, F> {
    fn run(mut self) -> Result<ScanReport> {
        let mut report = ScanReport {
            truncated_bytes: (self.truncated_base > 0).then_some(self.truncated_base),
            ..ScanReport::default()
        };
        let mut page_iter: u64 = 1;
        while page_iter < self.file_pages {
            let is_zero = self.load_is_zero_page(page_iter)?;
            if is_zero {
                // Everything after this must also be zero (free pre-allocation
                // space); verify and stop.
                report.free_tail_pages = self.file_pages - page_iter;
                for p in (page_iter + 1)..self.file_pages {
                    if !self.load_is_zero_page(p)? {
                        return Err(
                            self.corrupt(p, "non-zero page located after a zero (free) page")
                        );
                    }
                }
                break;
            }

            match self.process_page(page_iter, &mut report) {
                Ok(()) => page_iter += 1,
                Err(e) => {
                    if self.truncate_tail {
                        page_iter = self.truncate_from(page_iter, &mut report)?;
                        if page_iter >= self.file_pages {
                            break;
                        }
                        continue;
                    }
                    return Err(e);
                }
            }
        }

        if let Some(p) = &self.pending {
            // A record was in flight when the scan stopped: its continuation
            // pages never arrived. In repair mode the pages the record
            // touched (from its first fragment on) are cut so the kept
            // prefix holds completed records only — the resumed writer then
            // reuses the record's first page from scratch. A strict scan
            // reports the missing continuation instead.
            let (lsn, remaining, start_page) = {
                let header_lsn = p.header.lsn;
                let rem = p.remaining;
                (header_lsn, rem, p.start_page)
            };
            self.pending = None;
            if self.truncate_tail {
                self.cut_to(start_page, &mut report)?;
            } else {
                let reason = format!(
                    "file ends while record {lsn} still spans pages \
                     ({remaining} payload bytes missing)"
                );
                // pending records keep no state that outlives this scan
                return Err(self.corrupt(0, reason));
            }
        }

        report.record_count = self.records;
        report.max_lsn = self.last_completed;
        report.min_lsn = self.first_completed;
        Ok(report)
    }

    /// Loads one page; returns `true` when the page is entirely zero.
    fn load_is_zero_page(&mut self, page_index: u64) -> Result<bool> {
        self.db.read_page(page_index, &mut self.page)?;
        Ok(self.page.as_slice().iter().all(|&b| b == 0))
    }

    fn corrupt(&self, page_index: u64, reason: impl Into<String>) -> ChronoError {
        ChronoError::CorruptPage {
            page_index,
            reason: reason.into(),
        }
    }

    /// Cuts the file back to `keep` pages (keeping `0..keep`) and rolls the
    /// report back to the surviving prefix: pages the cut removed leave the
    /// report with the file (an in-flight record's fragment pages were
    /// committed as pages), and the continuity cursor re-anchors on the last
    /// surviving page. Returns the bytes removed.
    fn cut_to(&mut self, keep: u64, report: &mut ScanReport) -> Result<u64> {
        let cut = (self.file_pages - keep) * PAGE_SIZE_U64;
        self.db.truncate_to_len(keep * PAGE_SIZE_U64)?;
        self.file_pages = keep;
        self.pending = None;
        while report.pages.last().is_some_and(|p| p.page_index >= keep) {
            report.pages.pop();
        }
        self.prev_touched = report.pages.last().map_or(0, |p| p.max_lsn);
        report.truncated_bytes = Some(report.truncated_bytes.unwrap_or(0) + cut);
        Ok(cut)
    }

    /// Cuts the file back to `bad_page` pages (keep `0..bad_page`), reports
    /// the byte count removed, and returns the new page count. When a record
    /// is in flight its first fragment's page also goes — the kept prefix
    /// must contain completed records only.
    fn truncate_from(&mut self, bad_page: u64, report: &mut ScanReport) -> Result<u64> {
        let keep = match &self.pending {
            Some(p) => bad_page.min(p.start_page),
            None => bad_page,
        };
        self.cut_to(keep, report)
    }

    /// Truncate-or-propagate shortcut shared by all corruption sites.
    #[inline]
    fn decide(&mut self, bad_page: u64, e: ChronoError, report: &mut ScanReport) -> Result<()> {
        if self.truncate_tail {
            self.truncate_from(bad_page, report)?;
            Ok(())
        } else {
            Err(e)
        }
    }

    fn process_page(&mut self, page_index: u64, report: &mut ScanReport) -> Result<()> {
        let header = {
            let page: &[u8; PAGE_SIZE] = self.page.as_slice().try_into().expect("PAGE_SIZE page");
            match parse_page_header(page_index, page) {
                Ok(h) => h,
                Err(e) => return self.decide(page_index, e, report),
            }
        };

        // --- structural invariants =========================================
        if header.page_epoch != page_index {
            let e = self.corrupt(
                page_index,
                format!(
                    "page epoch {} does not match its file position",
                    header.page_epoch
                ),
            );
            return self.decide(page_index, e, report);
        }
        if header.prev_page_lsn != self.prev_touched {
            let e = self.corrupt(
                page_index,
                format!(
                    "prev_page_lsn {} breaks continuity (expected {})",
                    header.prev_page_lsn, self.prev_touched
                ),
            );
            return self.decide(page_index, e, report);
        }
        let starts_cont = header.flags & PAGE_FLAG_STARTS_WITH_CONTINUATION != 0;
        if starts_cont != self.pending.is_some() {
            let e = self.corrupt(
                page_index,
                if starts_cont {
                    "PAGE_FLAG_STARTS_WITH_CONTINUATION set but no record is in flight"
                } else {
                    "a record is in flight but this page misses the \
                     PAGE_FLAG_STARTS_WITH_CONTINUATION flag"
                },
            );
            return self.decide(page_index, e, report);
        }

        let mut off = PAGE_HEADER_SIZE;
        let mut fragments: u16 = 0;
        let mut used = PAGE_HEADER_SIZE;
        let mut page_max_lsn = self.prev_touched;

        // --- 1. consume the pending continuation fragment ====================
        let pending_completes_here = if let Some(pending) = self.pending.as_mut() {
            let free = (PAGE_SIZE - off) as u64;
            let take = free.min(pending.remaining) as usize;
            pending
                .hasher
                .update(&self.page.as_slice()[off..off + take]);
            self.span_payload
                .extend_from_slice(&self.page.as_slice()[off..off + take]);
            pending.remaining -= take as u64;
            page_max_lsn = pending.header.lsn;
            off += take;
            used = off;
            pending.remaining == 0
        } else {
            false
        };

        if pending_completes_here {
            fragments += 1;
            let pending = self.pending.take().expect("checked above");
            if pending.hasher.finish() != pending.header.record_crc {
                let e = ChronoError::RecordChecksumMismatch {
                    lsn: pending.header.lsn,
                };
                return self.decide(page_index, e, report);
            }
            let pages_spanned = page_index - pending.start_page + 1;
            let view = RecordView {
                header: pending.header,
                payload: &self.span_payload,
                start_page: pending.start_page,
                start_offset: pending.start_offset,
                pages_spanned,
            };
            self.records += 1;
            if self.first_completed.is_none() {
                self.first_completed = Some(view.lsn());
            }
            self.last_completed = Some(view.lsn());
            (self.visit)(view)?;
            self.span_payload.clear();
        } else if self.pending.is_some() {
            // Page filled up entirely mid-record; only page bookkeeping left.
            // The consumed continuation chunk is this page's one fragment.
            fragments += 1;
            return self.finish_page(page_index, header, fragments, used, page_max_lsn, report);
        }

        // --- 2. fresh records ===============================================
        // Walk exactly the fragments the page header declares; whatever
        // follows is padding and must be zero (checked in step 3).
        while off + RECORD_HEADER_SIZE <= PAGE_SIZE
            && (fragments as usize) < header.record_count as usize
        {
            if fragments == u16::MAX {
                let e = self.corrupt(page_index, "fragment count exceeds the u16 header capacity");
                return self.decide(page_index, e, report);
            }
            let page = self.page.as_slice();
            let raw = &page[off..PAGE_SIZE];
            let rec = match RecordHeader::decode(raw) {
                Ok(h) => h,
                Err(e) => return self.decide(page_index, e, report),
            };
            if rec.lsn != self.next_lsn {
                let e = ChronoError::NonMonotonicLsn {
                    previous: self.next_lsn.saturating_sub(1),
                    current: rec.lsn,
                };
                return self.decide(page_index, e, report);
            }
            if rec.timestamp_ns < self.last_ts {
                let e = ChronoError::NonMonotonicTimestamp {
                    previous: self.last_ts,
                    current: rec.timestamp_ns,
                };
                return self.decide(page_index, e, report);
            }

            let payload_size = rec.payload_size();
            let room = (PAGE_SIZE - off - RECORD_HEADER_SIZE) as u64;
            let take = (payload_size as u64).min(room) as usize;
            let complete_here = take as u64 == payload_size as u64;

            if complete_here {
                // Fully inside this page: zero-copy stream + emit.
                let crc = RecordHeader::compute_record_crc(
                    raw[..RECORD_HEADER_SIZE].try_into().unwrap(),
                    &page[off + RECORD_HEADER_SIZE..off + RECORD_HEADER_SIZE + take],
                );
                if crc != rec.record_crc {
                    let e = ChronoError::RecordChecksumMismatch { lsn: rec.lsn };
                    return self.decide(page_index, e, report);
                }
                fragments += 1;
                used = off + RECORD_HEADER_SIZE + take;
                page_max_lsn = rec.lsn;
                let view = RecordView {
                    header: rec,
                    payload: &page[off + RECORD_HEADER_SIZE..off + RECORD_HEADER_SIZE + take],
                    start_page: page_index,
                    start_offset: off,
                    pages_spanned: 1,
                };
                self.next_lsn = rec.lsn + 1;
                self.last_ts = rec.timestamp_ns;
                self.records += 1;
                if self.first_completed.is_none() {
                    self.first_completed = Some(view.lsn());
                }
                self.last_completed = Some(view.lsn());
                off += record_encoded_len(payload_size);
                (self.visit)(view)?;
            } else {
                // Continues on the next page(s).
                if !rec.continues_on_next_page() {
                    let midpage_lsn = rec.lsn;
                    let e = self.corrupt(
                        page_index,
                        format!(
                            "record {midpage_lsn} is truncated at the page boundary without \
                             REC_FLAG_CONTINUES_ON_NEXT_PAGE (payload {payload_size}, \
                             room {room})"
                        ),
                    );
                    return self.decide(page_index, e, report);
                }
                let mut hasher = Crc32cHasher::new();
                let encoded: [u8; RECORD_HEADER_SIZE] =
                    raw[..RECORD_HEADER_SIZE].try_into().unwrap();
                hasher.update(&encoded[..RH_OFF_RECORD_CRC]);
                hasher.update(&ZEROS4);
                hasher.update(&page[off + RECORD_HEADER_SIZE..off + RECORD_HEADER_SIZE + take]);
                self.span_payload.clear();
                self.span_payload.extend_from_slice(
                    &page[off + RECORD_HEADER_SIZE..off + RECORD_HEADER_SIZE + take],
                );
                self.pending = Some(Pending {
                    header: rec,
                    hasher,
                    remaining: payload_size as u64 - take as u64,
                    start_page: page_index,
                    start_offset: off,
                });
                fragments += 1;
                used = off + RECORD_HEADER_SIZE + take;
                page_max_lsn = rec.lsn;
                self.next_lsn = rec.lsn + 1;
                self.last_ts = rec.timestamp_ns;
                // Not a completed record yet: it only completes when its
                // continuation pages are all consumed.
                // The page is fully occupied by the first fragment.
                return self.finish_page(page_index, header, fragments, used, page_max_lsn, report);
            }
        }

        // --- 3. the payload tail must be zero filled =========================
        if off < PAGE_SIZE && self.page.as_slice()[off..].iter().any(|&b| b != 0) {
            let e = self.corrupt(
                page_index,
                "payload tail after the last record contains non-zero bytes",
            );
            return self.decide(page_index, e, report);
        }
        if fragments == 0 {
            let e = self.corrupt(page_index, "no record fragment in a data page");
            return self.decide(page_index, e, report);
        }
        self.commit_page(page_index, header, fragments, used, page_max_lsn, report)
    }

    /// Shared page bookkeeping used for both "record ends then page closes"
    /// and "page ends mid-record".
    fn finish_page(
        &mut self,
        page_index: u64,
        header: PageHeader,
        fragments: u16,
        used: usize,
        page_max_lsn: u64,
        report: &mut ScanReport,
    ) -> Result<()> {
        // The zero-fill check is skipped here only when the page is exactly
        // full (mid-record); a page closed with slack was checked by the
        // caller before entering this fn.
        if header.record_count != fragments {
            let e = self.corrupt(
                page_index,
                format!(
                    "record_count {} disagrees with the parsed fragment count {}",
                    header.record_count, fragments
                ),
            );
            return self.decide(page_index, e, report);
        }
        if header.flags & PAGE_FLAG_CLOSED == 0 {
            let e = self.corrupt(page_index, "on-disk page is not marked PAGE_FLAG_CLOSED");
            return self.decide(page_index, e, report);
        }
        report.pages.push(PageInfo {
            page_index,
            page_epoch: header.page_epoch,
            prev_page_lsn: header.prev_page_lsn,
            record_count: header.record_count,
            flags: header.flags,
            used_bytes: used,
            max_lsn: page_max_lsn,
        });
        self.prev_touched = page_max_lsn;
        Ok(())
    }

    fn commit_page(
        &mut self,
        page_index: u64,
        header: PageHeader,
        fragments: u16,
        used: usize,
        page_max_lsn: u64,
        report: &mut ScanReport,
    ) -> Result<()> {
        // when the record completes mid-page it leaves zero padding behind
        if header.record_count != fragments {
            let e = self.corrupt(
                page_index,
                format!(
                    "record_count {} disagrees with the parsed fragment count {}",
                    header.record_count, fragments
                ),
            );
            return self.decide(page_index, e, report);
        }
        if header.flags & PAGE_FLAG_CLOSED == 0 {
            let e = self.corrupt(page_index, "on-disk page is not marked PAGE_FLAG_CLOSED");
            return self.decide(page_index, e, report);
        }
        report.pages.push(PageInfo {
            page_index,
            page_epoch: header.page_epoch,
            prev_page_lsn: header.prev_page_lsn,
            record_count: header.record_count,
            flags: header.flags,
            used_bytes: used,
            max_lsn: page_max_lsn,
        });
        self.prev_touched = page_max_lsn;
        Ok(())
    }
}
