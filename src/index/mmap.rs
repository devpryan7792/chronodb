//! Read-only `MAP_SHARED` view of the database file (SPEC §3.1).
//!
//! Readers must never take the writer's locks, so the mmap manager exposes
//! plain slices into the page cache. Two invariants make that sound:
//!
//! * **regions are append-only**: a refresh never moves or unmaps existing
//!   mappings — it maps the newly grown tail (plus a bounded overlap) as an
//!   additional region. Slices handed out earlier stay valid for the whole
//!   engine lifetime; unmapping only happens in `Drop`.
//! * **the tail overlap keeps records contiguous**: a new region starts at
//!   `previous_end - overlap` where `overlap` ≥ the largest possible record
//!   extent, so a record can never straddle two regions (the proof mirrors
//!   the region math in `refresh`).
//!
//! Mappings are extended by a fixed slack chunk beyond the file length so
//! growth reuses the last region instead of stacking one mapping per batch.
//! Bytes inside the slack but beyond the real file length are *never*
//! exposed: [`MmapManager::slice`] additionally bounds every request by the
//! last observed file length, so a read can never touch a page past EOF
//! (`SIGBUS`).

use std::fs::File;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use parking_lot::RwLock;

use crate::error::{ChronoError, Result};
use crate::storage::page::PAGE_SIZE_U64;

/// Extra mapping slack past the file end; growth within this window reuses
/// the last region without a new `mmap` call.
const MAP_SLACK: u64 = 4 << 20;

/// One append-only mapped region `[start, start + len)` of the file.
struct Region {
    /// File offset the region starts at (page aligned).
    start: u64,
    /// Mapping extent in bytes (may include unwritten slack).
    len: usize,
    ptr: *const u8,
}

// SAFETY: `ptr` addresses an immutable `MAP_SHARED` mapping. The region list
// is append-only and mappings are released only in `Drop`, so both the
// pointer value and the memory it points to are stable for `&self`'s life.
unsafe impl Send for Region {}
unsafe impl Sync for Region {}

struct State {
    regions: Vec<Region>,
    /// Real file length observed at the last refresh — the upper bound of
    /// readable bytes (excludes the mapped slack).
    upto: u64,
}

/// File view for the read path. Cheap to share behind an `Arc`.
pub struct MmapManager {
    file: File,
    path: PathBuf,
    state: RwLock<State>,
    /// Tail overlap; must be ≥ the largest record extent so records never
    /// straddle region boundaries.
    overlap: u64,
    /// Diagnostics: total virtual bytes mapped across all regions.
    mapped_bytes: AtomicU64,
}

impl MmapManager {
    /// Opens `path` read-only. `overlap` must be at least the largest total
    /// record size (header + payload) the engine can produce.
    pub fn open(path: &Path, overlap: u64) -> Result<Self> {
        let file = File::open(path)?;
        Ok(Self {
            file,
            path: path.to_path_buf(),
            state: RwLock::new(State {
                regions: Vec::new(),
                upto: 0,
            }),
            overlap: overlap.max(PAGE_SIZE_U64),
            mapped_bytes: AtomicU64::new(0),
        })
    }

    /// Path of the mapped file.
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Maps any file bytes beyond the current coverage. Never unmaps; the
    /// writer thread is the only production caller. Returns the observed
    /// file length.
    pub fn refresh(&self) -> Result<u64> {
        let file_len = self.file.metadata().map_err(ChronoError::Io)?.len();
        let mut st = self.state.write();
        if file_len <= st.upto {
            return Ok(st.upto);
        }
        let last_extent_end = st.regions.last().map_or(0, |r| r.start + r.len as u64);
        if file_len > last_extent_end {
            // Start overlapping the previous coverage: a record ending in
            // the new range starts at ≥ end - max_record_extent, which the
            // overlap guarantees to be inside the new region.
            let start = align_down_page(st.upto.saturating_sub(self.overlap));
            let end = align_up_page(file_len).saturating_add(MAP_SLACK);
            let len = (end - start) as usize;
            let ptr = unsafe {
                libc::mmap(
                    std::ptr::null_mut(),
                    len,
                    libc::PROT_READ,
                    libc::MAP_SHARED,
                    std::os::unix::io::AsRawFd::as_raw_fd(&self.file),
                    start as libc::off_t,
                )
            };
            if ptr == libc::MAP_FAILED {
                return Err(ChronoError::Io(io::Error::last_os_error()));
            }
            st.regions.push(Region {
                start,
                len,
                ptr: ptr as *const u8,
            });
            self.mapped_bytes.fetch_add(len as u64, Ordering::Relaxed);
        }
        st.upto = file_len;
        Ok(file_len)
    }

    /// Bytes covered by the most recent refresh (excludes mapped slack).
    pub fn mapped_len(&self) -> u64 {
        self.state.read().upto
    }

    /// Virtual bytes mapped so far (diagnostics/tests).
    pub fn mapped_bytes(&self) -> u64 {
        self.mapped_bytes.load(Ordering::Relaxed)
    }

    /// Number of regions added so far (diagnostics/tests).
    pub fn region_count(&self) -> usize {
        self.state.read().regions.len()
    }

    /// Absolute end of the last region's mapping extent.
    pub fn mapped_extent_end(&self) -> u64 {
        self.state
            .read()
            .regions
            .last()
            .map_or(0, |r| r.start + r.len as u64)
    }

    /// Borrows `len` mapped bytes at absolute file `offset`, or `None` when
    /// the range is not (yet) covered. The returned slice points directly
    /// into the page cache and never includes bytes past the last observed
    /// file length.
    pub fn slice(&self, offset: u64, len: usize) -> Option<&[u8]> {
        let st = self.state.read();
        if len == 0 {
            return None;
        }
        let end = offset.checked_add(len as u64)?;
        // The real file bound excludes mapped slack: reads past EOF would
        // fault, bytes past EOF are meaningless.
        if end > st.upto {
            return None;
        }
        for region in st.regions.iter().rev() {
            let r_end = region.start + region.len as u64;
            if offset >= region.start && end <= r_end {
                let at = (offset - region.start) as usize;
                // SAFETY: bounds checked against the region's mapping; the
                // mapping is stable (never remapped/unmapped before drop)
                // and read-only, and `end <= upto` excludes slack pages.
                return Some(unsafe { std::slice::from_raw_parts(region.ptr.add(at), len) });
            }
        }
        None
    }
}

impl Drop for MmapManager {
    fn drop(&mut self) {
        for region in self.state.get_mut().regions.iter() {
            // SAFETY: each pointer/len pair comes from a successful mmap in
            // `refresh` and is released exactly once here.
            unsafe {
                libc::munmap(region.ptr as *mut libc::c_void, region.len);
            }
        }
    }
}

#[inline]
fn align_down_page(v: u64) -> u64 {
    v - (v % PAGE_SIZE_U64)
}

#[inline]
fn align_up_page(v: u64) -> u64 {
    v.div_ceil(PAGE_SIZE_U64) * PAGE_SIZE_U64
}

impl std::fmt::Debug for MmapManager {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MmapManager")
            .field("path", &self.path)
            .field("regions", &self.region_count())
            .field("upto", &self.mapped_len())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write as _;

    fn write_file(path: &Path, bytes: &[u8]) {
        let mut f = File::create(path).unwrap();
        f.write_all(bytes).unwrap();
        f.sync_all().unwrap();
    }

    fn append_file(path: &Path, bytes: &[u8]) {
        let mut f = std::fs::OpenOptions::new().append(true).open(path).unwrap();
        f.write_all(bytes).unwrap();
        f.sync_all().unwrap();
    }

    fn patterned(len: usize, seed: u64) -> Vec<u8> {
        (0..len)
            .map(|i| (i as u64).wrapping_mul(31).wrapping_add(seed) as u8)
            .collect()
    }

    #[test]
    fn maps_and_slices_the_initial_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("a.cdb");
        let data = patterned(3 * 4096 + 777, 1);
        write_file(&path, &data);

        let mm = MmapManager::open(&path, 4096).unwrap();
        assert_eq!(mm.mapped_len(), 0);
        assert!(mm.slice(0, 16).is_none());

        assert_eq!(mm.refresh().unwrap(), data.len() as u64);
        assert_eq!(mm.mapped_len(), data.len() as u64);
        assert_eq!(mm.region_count(), 1);
        assert_eq!(mm.slice(0, 16).unwrap(), &data[..16]);
        assert_eq!(mm.slice(4096, 100).unwrap(), &data[4096..4196]);
        assert_eq!(
            mm.slice(data.len() as u64 - 1, 1).unwrap(),
            &data[data.len() - 1..]
        );
        // Beyond the real file length — even though inside the mapped slack.
        assert!(mm.slice(data.len() as u64, 1).is_none());
        assert!(mm.slice(0, data.len() + 1).is_none());
        // Mapping extends past EOF for future growth.
        assert!(mm.mapped_extent_end() > data.len() as u64);
    }

    #[test]
    fn growth_within_slack_needs_no_new_region() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("b.cdb");
        write_file(&path, &patterned(4096, 2));

        let mm = MmapManager::open(&path, 4096).unwrap();
        mm.refresh().unwrap();
        assert_eq!(mm.region_count(), 1);

        append_file(&path, &patterned(64 * 1024, 3));
        mm.refresh().unwrap();
        assert_eq!(
            mm.region_count(),
            1,
            "growth inside the slack reuses the region"
        );
        assert_eq!(mm.mapped_len(), 4096 + 64 * 1024);
        assert_eq!(mm.slice(4096, 4).unwrap(), &patterned(64 * 1024, 3)[..4]);
    }

    #[test]
    fn growth_beyond_slack_adds_a_region_and_keeps_old_slices_valid() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("c.cdb");
        let first = patterned(4096, 4);
        write_file(&path, &first);

        let mm = MmapManager::open(&path, 4096).unwrap();
        mm.refresh().unwrap();
        let head: &[u8] = mm.slice(0, 32).unwrap();
        assert_eq!(head, &first[..32]);

        // Jump well past the initial mapping extent (slack is 4 MiB).
        let second = patterned(6 << 20, 5);
        append_file(&path, &second);
        mm.refresh().unwrap();

        assert!(mm.region_count() >= 2);
        assert_eq!(mm.mapped_len(), (4096 + second.len()) as u64);
        assert_eq!(
            mm.slice(0, 32).unwrap(),
            &first[..32],
            "old region still valid"
        );
        assert_eq!(head, &first[..32], "earlier slice still valid");
        assert_eq!(mm.slice(4096, 8).unwrap(), &second[..8]);
        assert_eq!(
            mm.slice((4096 + second.len() - 4) as u64, 4).unwrap(),
            &second[second.len() - 4..]
        );
    }

    #[test]
    fn records_straddling_a_region_boundary_read_contiguously() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("d.cdb");
        let a_len = 1 << 20;
        let a = patterned(a_len, 6);
        write_file(&path, &a);

        let mm = MmapManager::open(&path, 4096).unwrap();
        mm.refresh().unwrap();
        let boundary = mm.mapped_extent_end();
        assert!(boundary > a_len as u64);

        // Grow past the mapped extent so a second region is created, then
        // read a "record" that straddles the first region's end…
        let b = patterned(5 << 20, 7);
        append_file(&path, &b);
        mm.refresh().unwrap();
        assert!(mm.region_count() >= 2);

        let start = boundary - 50;
        let expected: Vec<u8> = (start..start + 100)
            .map(|i| {
                if i < a_len as u64 {
                    a[i as usize]
                } else {
                    b[(i - a_len as u64) as usize]
                }
            })
            .collect();
        assert_eq!(
            mm.slice(start, 100).unwrap(),
            &expected[..],
            "…must be one contiguous slice"
        );
        // …while a range crossing the *last* region's extent end is absent.
        let past = mm.mapped_extent_end() - 10;
        assert!(mm.slice(past, 100).is_none());
    }

    #[test]
    fn empty_file_maps_nothing() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("e.cdb");
        write_file(&path, &[]);
        let mm = MmapManager::open(&path, 4096).unwrap();
        assert_eq!(mm.refresh().unwrap(), 0);
        assert_eq!(mm.region_count(), 0);
        assert!(mm.slice(0, 1).is_none());
    }
}
