//! Low-level database file (`*.cdb`) I/O.
//!
//! * page-`PAGE_SIZE`-aligned scratch buffers ([`AlignedBuf`]) so `O_DIRECT`
//!   reads/writes are always properly aligned,
//! * [`DbFile`] — create/open/validate, `posix_fallocate` pre-allocation with
//!   a sparse fallback, positioned page reads/writes, `fdatasync`
//!   durability and truncation.

use std::alloc::{alloc_zeroed, dealloc, Layout};
use std::fs::{File, OpenOptions};
use std::io;
use std::ops::{Deref, DerefMut};
use std::os::unix::fs::{FileExt, OpenOptionsExt};
use std::os::unix::io::AsRawFd;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use crate::error::{ChronoError, Result};
use crate::storage::page::{page_offset, Superblock, PAGE_SIZE, SB_FLAG_STRICT_DIRECT_IO};

// ---------------------------------------------------------------------------
// aligned scratch buffers
// ---------------------------------------------------------------------------

/// A zero-initialised, caller-fixed-alignment allocation. Frees itself.
pub struct AlignedBuf {
    layout: Layout,
    ptr: *mut u8,
    len: usize,
}

impl Deref for AlignedBuf {
    type Target = [u8];
    #[inline]
    fn deref(&self) -> &[u8] {
        self.as_slice()
    }
}
impl DerefMut for AlignedBuf {
    #[inline]
    fn deref_mut(&mut self) -> &mut [u8] {
        self.as_mut_slice()
    }
}

// The buffer is plain bytes used by a single owning struct at a time; no
// interior mutability, so these are sound.
unsafe impl Send for AlignedBuf {}
unsafe impl Sync for AlignedBuf {}

impl AlignedBuf {
    /// Allocates a zeroed buffer of `len` bytes with `align` alignment.
    pub fn zeroed(len: usize, align: usize) -> Result<Self> {
        let layout = Layout::from_size_align(len, align)
            .map_err(|e| ChronoError::Io(io::Error::new(io::ErrorKind::InvalidInput, e)))?;
        let ptr = unsafe { alloc_zeroed(layout) };
        if ptr.is_null() {
            return Err(ChronoError::Io(io::Error::new(
                io::ErrorKind::OutOfMemory,
                format!("failed to allocate {len} bytes aligned to {align}"),
            )));
        }
        Ok(Self { layout, ptr, len })
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.len
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    #[inline]
    pub fn as_slice(&self) -> &[u8] {
        unsafe { std::slice::from_raw_parts(self.ptr, self.len) }
    }

    #[inline]
    pub fn as_mut_slice(&mut self) -> &mut [u8] {
        unsafe { std::slice::from_raw_parts_mut(self.ptr, self.len) }
    }

    #[inline]
    pub fn as_ptr(&self) -> *const u8 {
        self.ptr
    }

    #[inline]
    pub fn as_mut_ptr(&mut self) -> *mut u8 {
        self.ptr
    }

    /// True when the buffer start is aligned to `align` bytes.
    #[inline]
    pub fn is_aligned_to(&self, align: usize) -> bool {
        (self.ptr as usize) % align.max(1) == 0
    }
}

impl Drop for AlignedBuf {
    fn drop(&mut self) {
        unsafe { dealloc(self.ptr, self.layout) };
    }
}

impl AsRef<[u8]> for AlignedBuf {
    fn as_ref(&self) -> &[u8] {
        self.as_slice()
    }
}
impl AsMut<[u8]> for AlignedBuf {
    fn as_mut(&mut self) -> &mut [u8] {
        self.as_mut_slice()
    }
}

// ---------------------------------------------------------------------------
// cluster identifier
// ---------------------------------------------------------------------------

/// Distinctly random: 16 bytes of OS entropy, with a deterministic
/// (time ^ pid ^ ASLR address) mixing fallback if `/dev/urandom` is
/// unreadable. Never zero.
pub fn generate_cluster_id() -> u128 {
    let mut id = [0u8; 16];
    if let Ok(mut f) = File::open("/dev/urandom") {
        if io::Read::read_exact(&mut f, &mut id).is_ok() {
            let v = u128::from_ne_bytes(id);
            if v != 0 {
                return v;
            }
        }
    }
    // SplitMix64 mixing of three independent-ish sources.
    let time = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0);
    let pid = std::process::id() as u64;
    let addr = &time as *const u64 as u64;
    let mut mix = time ^ pid.rotate_left(32) ^ addr;
    let mut out = 0u128;
    for i in 0..2 {
        mix = mix
            .wrapping_add(0x9E37_79B9_7F4A_7C15)
            .wrapping_mul(0xBF58_476D_1CE4_E5B9)
            .wrapping_mul(0x94D0_49BB_1331_11EB);
        out |= (mix as u128) << (64 * i);
    }
    out
}

/// Wall-clock nanoseconds since the Unix epoch (0 once the clock is sane).
pub(crate) fn now_ns() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0)
}

// ---------------------------------------------------------------------------
// DbFile
// ---------------------------------------------------------------------------

/// Pre-allocation strategy actually used by the filesystem.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AllocMode {
    /// Nothing was pre-allocated (preallocate pages == 0).
    None,
    /// `posix_fallocate(3)` — guaranteed non-sparse, zero on first read.
    Fallocate,
    /// `ftruncate` fallback — sparse, no allocation guarantee.
    SetLen,
}

/// Options for [`DbFile::create`].
#[derive(Clone, Copy, Debug)]
pub struct CreateOptions {
    /// Pages to pre-allocate when the file is created (superblock included).
    pub preallocate_pages: u64,
    /// Open the live file with `O_DIRECT` (recorded as superblock flag 0).
    pub strict_direct_io: bool,
}

impl Default for CreateOptions {
    fn default() -> Self {
        Self {
            preallocate_pages: 256,
            strict_direct_io: false,
        }
    }
}

/// Read-write or read-only handle on a `*.cdb` file.
pub struct DbFile {
    file: File,
    path: PathBuf,
    direct_io: bool,
    read_only: bool,
    alloc_mode: AllocMode,
}

impl DbFile {
    // -- construction -------------------------------------------------------

    /// Creates a brand-new database: page 0 carries the superblock, then the
    /// data pages are pre-allocated.
    pub fn create(path: &Path, opts: &CreateOptions) -> Result<(Self, Superblock)> {
        if path.exists() {
            return Err(ChronoError::Io(io::Error::new(
                io::ErrorKind::AlreadyExists,
                format!("refusing to overwrite existing file {}", path.display()),
            )));
        }
        let mut flags = 0;
        if opts.strict_direct_io {
            flags |= SB_FLAG_STRICT_DIRECT_IO;
        }
        let sb = Superblock::new(generate_cluster_id(), flags, now_ns());

        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(path)?;

        let mut sb_page = AlignedBuf::zeroed(PAGE_SIZE, PAGE_SIZE)?;
        sb.encode(sb_page.as_mut_slice().try_into().expect("PAGE_SIZE"));
        file.write_all_at(sb_page.as_slice(), 0)?;

        let mut db = Self {
            file,
            path: path.to_path_buf(),
            direct_io: false,
            read_only: false,
            alloc_mode: AllocMode::None,
        };
        if opts.preallocate_pages > 0 {
            db.preallocate(opts.preallocate_pages)?;
        }
        if opts.strict_direct_io {
            // Reopen the live handle so metadata ops (create) do not pay the
            // O_DIRECT alignment tax, but all data pages do.
            db.reopen(true)?;
        }
        db.sync_all()?; // durable: superblock + file size before any appends
        Ok((db, sb))
    }

    /// Opens an existing database for reading and writing after a full
    /// superblock validation (SPEC §5.2 step 1).
    pub fn open(path: &Path) -> Result<(Self, Superblock)> {
        let (file, sb) = open_validated(path, false, false)?;
        Ok((
            Self {
                file,
                path: path.to_path_buf(),
                direct_io: false,
                read_only: false,
                alloc_mode: AllocMode::None,
            },
            sb,
        ))
    }

    /// Same as [`DbFile::open`] but additionally opens the live handle with
    /// `O_DIRECT`.
    pub fn open_direct(path: &Path) -> Result<(Self, Superblock)> {
        let (file, sb) = open_validated(path, false, true)?;
        Ok((
            Self {
                file,
                path: path.to_path_buf(),
                direct_io: true,
                read_only: false,
                alloc_mode: AllocMode::None,
            },
            sb,
        ))
    }

    /// Opens for inspection only.
    pub fn open_read_only(path: &Path) -> Result<(Self, Superblock)> {
        let (file, sb) = open_validated(path, true, false)?;
        Ok((
            Self {
                file,
                path: path.to_path_buf(),
                direct_io: false,
                read_only: true,
                alloc_mode: AllocMode::None,
            },
            sb,
        ))
    }

    fn reopen(&mut self, direct: bool) -> Result<()> {
        let mut o = OpenOptions::new();
        o.read(true)
            .write(true)
            .custom_flags(if direct { O_DIRECT } else { 0 });
        let f = o.open(&self.path)?;
        self.file = f;
        self.direct_io = direct;
        Ok(())
    }

    // -- introspection -------------------------------------------------------

    pub fn path(&self) -> &Path {
        &self.path
    }
    pub fn is_direct_io(&self) -> bool {
        self.direct_io
    }
    pub fn is_read_only(&self) -> bool {
        self.read_only
    }
    pub fn alloc_mode(&self) -> AllocMode {
        self.alloc_mode
    }

    /// Current length of the backing file.
    pub fn len(&self) -> Result<u64> {
        Ok(self.file.metadata()?.len())
    }

    pub fn is_empty(&self) -> Result<bool> {
        Ok(self.len()? == 0)
    }

    // -- pre-allocation -------------------------------------------------------

    /// Grows the file to `pages` pages total using `posix_fallocate`.
    /// Filesystems that do not implement allocation (e.g. NFS, FAT) fall
    /// back to `ftruncate` (sparse) and warn by recording it in
    /// [`DbFile::alloc_mode`].
    pub fn preallocate(&mut self, pages: u64) -> Result<u64> {
        let bytes = pages
            .checked_mul(crate::storage::page::PAGE_SIZE_U64)
            .expect("preallocation size overflow");
        if bytes == 0 {
            self.alloc_mode = AllocMode::None;
            return Ok(0);
        }
        let fd = self.file.as_raw_fd();
        let rc = unsafe { libc::posix_fallocate(fd, 0, bytes as libc::off_t) };
        if rc == 0 {
            self.sync_all()?; // persist the grown size
            self.alloc_mode = AllocMode::Fallocate;
            return Ok(bytes);
        }
        if rc == libc::EOPNOTSUPP || rc == libc::ENOSYS {
            self.file.set_len(bytes)?;
            self.sync_all()?;
            self.alloc_mode = AllocMode::SetLen;
            return Ok(bytes);
        }
        Err(io::Error::from_raw_os_error(rc).into())
    }

    // -- positioned page I/O ---------------------------------------------------

    /// Reads exactly one page into `buf[0..PAGE_SIZE]`.
    pub fn read_page(&self, page_index: u64, buf: &mut AlignedBuf) -> Result<()> {
        self.ensure_writable_target(page_index, buf)?;
        let mut pos = 0usize;
        let want = PAGE_SIZE;
        let whole = buf.as_mut_slice();
        while pos < want {
            let n = self
                .file
                .read_at(&mut whole[pos..want], page_offset(page_index) + pos as u64)?;
            if n == 0 {
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    format!("short read at page {page_index}"),
                )
                .into());
            }
            pos += n;
        }
        Ok(())
    }

    fn ensure_writable_target(&self, page_index: u64, buf: &AlignedBuf) -> Result<()> {
        if buf.len() < PAGE_SIZE {
            return Err(ChronoError::Io(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("read buffer needs {} bytes, got {}", PAGE_SIZE, buf.len()),
            )));
        }
        if buf.is_aligned_to(PAGE_SIZE) {
            // aligned buffers are mandatory for O_DIRECT reads
        } else if self.direct_io {
            return Err(ChronoError::Io(io::Error::new(
                io::ErrorKind::InvalidInput,
                "O_DIRECT requires page-aligned buffers",
            )));
        }
        let read_only_check = self.read_only; // reads are fine read-only
        let _ = read_only_check;
        let _ = page_index;
        Ok(())
    }

    /// Writes `pages` consecutive pages starting at `page_index`, from the
    /// aligned buffer.
    pub fn write_pages(&self, page_index: u64, buf: &AlignedBuf, pages: usize) -> Result<()> {
        if self.read_only {
            return Err(ChronoError::Io(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "database opened read-only",
            )));
        }
        if pages == 0 {
            return Ok(());
        }
        let len = pages * PAGE_SIZE;
        if buf.len() < len {
            return Err(ChronoError::Io(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("write buffer needs {len} bytes, got {}", buf.len()),
            )));
        }
        if (page_offset(page_index) % crate::storage::page::PAGE_SIZE_U64) != 0 {
            return Err(ChronoError::Io(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("write offset {page_index} not page aligned"),
            )));
        }
        if self.direct_io && !buf.is_aligned_to(PAGE_SIZE) {
            return Err(ChronoError::Io(io::Error::new(
                io::ErrorKind::InvalidInput,
                "O_DIRECT requires page-aligned buffers",
            )));
        }
        let mut written = 0usize;
        while written < len {
            written += self.file.write_at(
                &buf.as_slice()[written..len],
                page_offset(page_index) + written as u64,
            )?;
        }
        Ok(())
    }

    /// Shrinks (or extends) the file to `len` bytes — crash recovery's tail
    /// truncation primitive.
    pub fn truncate_to_len(&self, len: u64) -> Result<()> {
        if self.read_only {
            return Err(ChronoError::Io(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "database opened read-only",
            )));
        }
        self.file.set_len(len)?;
        self.sync_all()?;
        Ok(())
    }

    // -- durability ------------------------------------------------------------

    /// `fdatasync(2)` — page data and its size without metadata rewrites.
    pub fn sync_data(&self) -> Result<()> {
        self.file.sync_data()?;
        Ok(())
    }

    /// `fsync(2)` — page data plus every metadata field (size, etc.).
    pub fn sync_all(&self) -> Result<()> {
        self.file.sync_all()?;
        Ok(())
    }
}

/// `O_DIRECT` constant (Linux x86_64/aarch64 value, mirrored by libc).
const O_DIRECT: i32 = 0o40000;

fn open_validated(path: &Path, read_only: bool, direct: bool) -> Result<(File, Superblock)> {
    let mut o = OpenOptions::new();
    o.read(true);
    if !read_only {
        o.write(true);
        if direct {
            o.custom_flags(O_DIRECT);
        }
    }
    let file = o.open(path)?;
    let mut sb_page = AlignedBuf::zeroed(PAGE_SIZE, PAGE_SIZE)?;
    let mut pos = 0usize;
    while pos < PAGE_SIZE {
        let tmp = sb_page.as_mut_slice();
        let n = file.read_at(&mut tmp[pos..PAGE_SIZE], pos as u64)?;
        if n == 0 {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                format!("{} is shorter than the superblock", path.display()),
            )
            .into());
        }
        pos += n;
    }
    let sb_arr: [u8; PAGE_SIZE] = sb_page
        .as_slice()
        .try_into()
        .expect("superblock page is exactly PAGE_SIZE");
    let sb = Superblock::decode(&sb_arr)?;
    Ok((file, sb))
}

impl io::Write for DbFile {
    /// Sequential full write used by secrets-free tests; not for production.
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.file.write(buf)
    }
    fn flush(&mut self) -> io::Result<()> {
        self.file.flush()
    }
}
