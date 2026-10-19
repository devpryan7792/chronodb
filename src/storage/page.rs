//! 4096-byte page primitives: the superblock (page 0) and the per-page header
//! that precedes every record page, plus the page-level CRC32-C protocol:
//!
//! * superblock checksum covers bytes `0x000..0xFFC` (everything before the
//!   4-byte checksum field) — SPEC §2.2.
//! * page checksum covers the full 4096-byte page with the checksum field at
//!   bytes `20..24` treated as zero.

use crate::error::{ChronoError, Result};
use crate::storage::crc::{crc32c, Crc32cHasher};
use byteorder::{ByteOrder, LittleEndian};

/// Fixed page size for every ChronoDB file (SPEC §2).
pub const PAGE_SIZE: usize = 4096;
pub const PAGE_SIZE_U64: u64 = PAGE_SIZE as u64;

/// ASCII `"CHRONODB"` (SPEC §2.2).
pub const MAGIC: [u8; 8] = *b"CHRONODB";
/// Current on-disk format version (SPEC §2.2).
pub const FORMAT_VERSION: u32 = 1;

// Superblock field offsets (SPEC §2.2).
const SB_OFF_VERSION: usize = 0x08;
const SB_OFF_PAGE_SIZE: usize = 0x0C;
const SB_OFF_CREATED_AT: usize = 0x10;
const SB_OFF_CLUSTER_ID: usize = 0x18;
const SB_OFF_FLAGS: usize = 0x28;
const SB_OFF_RESERVED: usize = 0x2C;
/// Zero-padded region between the flags field and the checksum (SPEC §2.2).
const SB_RESERVED_LEN: usize = 4048;
const SB_OFF_CHECKSUM: usize = 0xFFC;

/// Superblock flag: writer opened the file with `O_DIRECT` (SPEC §2.2).
pub const SB_FLAG_STRICT_DIRECT_IO: u32 = 1 << 0;
/// Superblock flag: payloads are compressed (SPEC §2.2; reserved for later).
pub const SB_FLAG_COMPRESSED_PAYLOADS: u32 = 1 << 1;

// Page header field offsets (SPEC §2.3 plus the trailing pad that promotes
// the header to 32 bytes — see repo plan decision).
pub const PAGE_HEADER_SIZE: usize = 32;
/// Bytes available for records inside one page.
pub const PAGE_PAYLOAD_SIZE: usize = PAGE_SIZE - PAGE_HEADER_SIZE;

const PH_OFF_EPOCH: usize = 0;
const PH_OFF_PREV_LSN: usize = 8;
const PH_OFF_RECORD_COUNT: usize = 16;
const PH_OFF_FLAGS: usize = 18;
const PH_OFF_CRC: usize = 20;
const PH_OFF_RESERVED: usize = 24;
const PH_OFF_PAD: usize = 28;

/// Page flag: nothing else will ever be appended to this page; the payload
/// tail between the last fragment and EOF-of-page is zero filled.
pub const PAGE_FLAG_CLOSED: u16 = 1 << 0;
/// Page flag: the first fragment in this page belongs to a record whose
/// header lives on the previous page.
pub const PAGE_FLAG_STARTS_WITH_CONTINUATION: u16 = 1 << 1;

// ---------------------------------------------------------------------------
// superblock
// ---------------------------------------------------------------------------

/// Validated, in-memory view of page 0 (SPEC §2.2).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Superblock {
    version: u32,
    page_size: u32,
    created_at_ns: u64,
    cluster_id: u128,
    flags: u32,
}

impl Superblock {
    /// Creates the values for a fresh database.
    pub fn new(cluster_id: u128, flags: u32, created_at_ns: u64) -> Self {
        Self {
            version: FORMAT_VERSION,
            page_size: PAGE_SIZE as u32,
            created_at_ns,
            cluster_id,
            flags,
        }
    }

    pub fn version(&self) -> u32 {
        self.version
    }
    pub fn page_size(&self) -> u32 {
        self.page_size
    }
    pub fn created_at_ns(&self) -> u64 {
        self.created_at_ns
    }
    pub fn cluster_id(&self) -> u128 {
        self.cluster_id
    }
    pub fn flags(&self) -> u32 {
        self.flags
    }
    pub fn strict_direct_io(&self) -> bool {
        self.flags & SB_FLAG_STRICT_DIRECT_IO != 0
    }

    /// Serialises into `out` (must be exactly 4096 bytes), leaving the whole
    /// block zero-padded beyond the checksum field.
    pub fn encode(&self, out: &mut [u8; PAGE_SIZE]) {
        out.fill(0);
        out[0..8].copy_from_slice(&MAGIC);
        LittleEndian::write_u32(&mut out[SB_OFF_VERSION..], self.version);
        LittleEndian::write_u32(&mut out[SB_OFF_PAGE_SIZE..], self.page_size);
        LittleEndian::write_u64(&mut out[SB_OFF_CREATED_AT..], self.created_at_ns);
        LittleEndian::write_u128(&mut out[SB_OFF_CLUSTER_ID..], self.cluster_id);
        LittleEndian::write_u32(&mut out[SB_OFF_FLAGS..], self.flags);
        let checksum = crc32c(&out[..SB_OFF_CHECKSUM]);
        LittleEndian::write_u32(&mut out[SB_OFF_CHECKSUM..], checksum);
    }

    /// Byte-exact inverse of [`Superblock::encode`] with full validation.
    pub fn decode(page: &[u8; PAGE_SIZE]) -> Result<Self> {
        if page[0..8] != MAGIC {
            return Err(ChronoError::CorruptSuperblock {
                reason: format!(
                    "magic mismatch: expected {MAGIC:?}, found {:?}",
                    &page[0..8]
                ),
            });
        }
        let stored = LittleEndian::read_u32(&page[SB_OFF_CHECKSUM..]);
        let computed = crc32c(&page[..SB_OFF_CHECKSUM]);
        if stored != computed {
            return Err(ChronoError::CorruptSuperblock {
                reason: format!(
                    "checksum mismatch: stored {stored:#010x}, computed {computed:#010x}"
                ),
            });
        }
        let version = LittleEndian::read_u32(&page[SB_OFF_VERSION..]);
        if version != FORMAT_VERSION {
            return Err(ChronoError::CorruptSuperblock {
                reason: format!("unsupported format version {version} (expected {FORMAT_VERSION})"),
            });
        }
        let page_size = LittleEndian::read_u32(&page[SB_OFF_PAGE_SIZE..]);
        if page_size != PAGE_SIZE as u32 {
            return Err(ChronoError::CorruptSuperblock {
                reason: format!("unsupported page size {page_size} (expected {PAGE_SIZE})"),
            });
        }
        if page[SB_OFF_RESERVED..SB_OFF_CHECKSUM] != [0u8; SB_RESERVED_LEN] {
            return Err(ChronoError::CorruptSuperblock {
                reason: "reserved region is not zero-filled".to_string(),
            });
        }
        Ok(Self {
            version,
            page_size,
            created_at_ns: LittleEndian::read_u64(&page[SB_OFF_CREATED_AT..]),
            cluster_id: LittleEndian::read_u128(&page[SB_OFF_CLUSTER_ID..]),
            flags: LittleEndian::read_u32(&page[SB_OFF_FLAGS..]),
        })
    }
}

const ZEROS4: [u8; 4] = [0; 4];

// ---------------------------------------------------------------------------
// page header
// ---------------------------------------------------------------------------

/// Validated view of the first 32 bytes of every record page (SPEC §2.3,
/// extended with a trailing pad word to fill 32 bytes; both reserved words
/// are zero on write and verified on read).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PageHeader {
    /// Absolute page ordinal; must equal the page number in the file.
    pub page_epoch: u64,
    /// Max LSN among records touching the previous page (0 for page 1).
    pub prev_page_lsn: u64,
    /// Number of record fragments stored in this page.
    pub record_count: u16,
    /// [`PAGE_FLAG_CLOSED`] | [`PAGE_FLAG_STARTS_WITH_CONTINUATION`].
    pub flags: u16,
}

impl PageHeader {
    /// Header for a fresh page.
    pub fn new(page_epoch: u64, prev_page_lsn: u64) -> Self {
        Self {
            page_epoch,
            prev_page_lsn,
            record_count: 0,
            flags: 0,
        }
    }

    /// Writes the 32-byte header into `page`, zeroing the checksum and both
    /// reserved words first.
    pub fn write(&self, page: &mut [u8; PAGE_SIZE]) {
        page[PH_OFF_EPOCH..PH_OFF_CRC + 4].fill(0);
        LittleEndian::write_u64(&mut page[PH_OFF_EPOCH..], self.page_epoch);
        LittleEndian::write_u64(&mut page[PH_OFF_PREV_LSN..], self.prev_page_lsn);
        LittleEndian::write_u16(&mut page[PH_OFF_RECORD_COUNT..], self.record_count);
        LittleEndian::write_u16(&mut page[PH_OFF_FLAGS..], self.flags);
        LittleEndian::write_u32(&mut page[PH_OFF_CRC..], 0);
        LittleEndian::write_u32(&mut page[PH_OFF_RESERVED..], 0);
        LittleEndian::write_u32(&mut page[PH_OFF_PAD..], 0);
    }

    /// Reads header fields back out of a page whose checksum already passed
    /// verification.
    pub fn read_unchecked(page: &[u8; PAGE_SIZE]) -> Self {
        Self {
            page_epoch: LittleEndian::read_u64(&page[PH_OFF_EPOCH..]),
            prev_page_lsn: LittleEndian::read_u64(&page[PH_OFF_PREV_LSN..]),
            record_count: LittleEndian::read_u16(&page[PH_OFF_RECORD_COUNT..]),
            flags: LittleEndian::read_u16(&page[PH_OFF_FLAGS..]),
        }
    }
}

/// Computes the page checksum over the whole 4096-byte block with the
/// checksum field zeroed, then stores it at bytes `20..24`.
pub fn stamp_page_crc(page: &mut [u8; PAGE_SIZE]) -> u32 {
    page[PH_OFF_CRC..PH_OFF_CRC + 4].fill(0);
    let crc = crc32c(page);
    page[PH_OFF_CRC..PH_OFF_CRC + 4].copy_from_slice(&crc.to_le_bytes());
    crc
}

/// Streaming verifier mirroring [`stamp_page_crc`] — never materialises a
/// temporary buffer.
pub fn verify_page_crc(page_index: u64, page: &[u8; PAGE_SIZE]) -> Result<u32> {
    let stored = LittleEndian::read_u32(&page[PH_OFF_CRC..]);
    let mut h = Crc32cHasher::new();
    h.update(&page[..PH_OFF_CRC]);
    h.update(&ZEROS4);
    h.update(&page[PH_OFF_CRC + 4..PAGE_SIZE]);
    let computed = h.finish();
    if computed != stored {
        return Err(ChronoError::PageChecksumMismatch {
            page_index,
            stored,
            computed,
        });
    }
    Ok(stored)
}

/// Parses and fully validates a page: checksum plus reserved-word zeroing.
pub fn parse_page_header(page_index: u64, page: &[u8; PAGE_SIZE]) -> Result<PageHeader> {
    verify_page_crc(page_index, page)?;
    if LittleEndian::read_u32(&page[PH_OFF_RESERVED..]) != 0
        || LittleEndian::read_u32(&page[PH_OFF_PAD..]) != 0
    {
        return Err(ChronoError::CorruptPage {
            page_index,
            reason: "page header reserved words are dirty".to_string(),
        });
    }
    Ok(PageHeader::read_unchecked(page))
}

// ---------------------------------------------------------------------------
// offsets / arithmetic
// ---------------------------------------------------------------------------

/// Byte offset of a page inside the database file.
#[inline]
pub fn page_offset(page_index: u64) -> u64 {
    page_index * PAGE_SIZE_U64
}

/// Page index holding a byte offset.
#[inline]
pub fn page_of(offset: u64) -> u64 {
    offset / PAGE_SIZE_U64
}

#[inline]
pub fn is_page_aligned(len: u64) -> bool {
    len % PAGE_SIZE_U64 == 0
}

/// Smallest page-aligned length ≥ `len`.
#[inline]
pub fn align_up(len: u64) -> u64 {
    len.div_ceil(PAGE_SIZE_U64) * PAGE_SIZE_U64
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn superblock_roundtrip() {
        let sb = Superblock::new(0x0123_4567_89ab_cdef_1122_3344_5566_7788, 1, 1234567);
        let mut page = [0u8; PAGE_SIZE];
        sb.encode(&mut page);
        assert_eq!(&page[0..8], &MAGIC);
        assert_eq!(LittleEndian::read_u32(&page[0x08..]), FORMAT_VERSION);
        assert_eq!(LittleEndian::read_u32(&page[0x0C..]), 4096);
        assert_eq!(LittleEndian::read_u64(&page[0x10..]), 1234567);
        assert_eq!(LittleEndian::read_u128(&page[0x18..]), sb.cluster_id());
        assert_eq!(page[0x2C..0xFFC], [0u8; SB_RESERVED_LEN]);
        let back = Superblock::decode(&page).unwrap();
        assert_eq!(sb, back);
    }

    #[test]
    fn superblock_rejects_bad_magic() {
        let mut page = [0u8; PAGE_SIZE];
        Superblock::new(1, 0, 1).encode(&mut page);
        page[3] ^= 0x55;
        assert!(matches!(
            Superblock::decode(&page),
            Err(ChronoError::CorruptSuperblock { .. })
        ));
    }

    #[test]
    fn superblock_checksum_covers_reserved_region() {
        let mut page = [0u8; PAGE_SIZE];
        Superblock::new(2, 0, 2).encode(&mut page);
        page[0x800] ^= 1; // deep inside the reserved region
        assert!(Superblock::decode(&page).is_err());
    }

    #[test]
    fn page_header_roundtrip_and_crc() {
        let mut page = [0u8; PAGE_SIZE];
        let hdr = PageHeader::new(3, 42);
        hdr.write(&mut page);
        for (i, fill) in page[PAGE_HEADER_SIZE..].iter_mut().enumerate() {
            *fill = (i % 251) as u8;
        }
        assert!(stamp_page_crc(&mut page) != 0);
        let back = parse_page_header(3, &page).unwrap();
        assert_eq!(hdr, back);
        // reserved words stay clean
        assert_eq!(&page[24..28], &[0; 4]);
        assert_eq!(&page[28..32], &[0; 4]);
    }

    #[test]
    fn page_crc_detects_single_bit_flip() {
        let mut page = [0u8; PAGE_SIZE];
        PageHeader::new(1, 0).write(&mut page);
        page[100] = 0xAA;
        stamp_page_crc(&mut page);
        for pos in [50usize, 100, 2000, 4095, 21] {
            let mut broken = page;
            broken[pos] ^= 0x01;
            assert!(
                verify_page_crc(1, &broken).is_err(),
                "flip at {pos} not caught"
            );
        }
    }

    #[test]
    fn arithmetic_helpers() {
        assert_eq!(page_offset(3), 12_288);
        assert_eq!(page_of(12_288), 3);
        assert!(is_page_aligned(0) && is_page_aligned(4096));
        assert!(!is_page_aligned(4097));
        assert_eq!(align_up(0), 0);
        assert_eq!(align_up(1), 4096);
        assert_eq!(align_up(4096), 4096);
        assert_eq!(align_up(4097), 8192);
    }
}
