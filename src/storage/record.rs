//! Event record wire format (SPEC §2.4): a 44-byte packed header followed
//! contiguously by its payload. All little-endian.
//!
//! Record checksum protocol: CRC32-C over the 44-byte header image with the
//! `record_crc` field (bytes `40..44`) treated as zero, followed by the
//! payload bytes.

use crate::error::{ChronoError, Result};
use crate::storage::crc::Crc32cHasher;
use byteorder::{ByteOrder, LittleEndian as LE};

/// Size of [`RecordHeader`] on the wire (SPEC §2.4 field sum).
pub const RECORD_HEADER_SIZE: usize = 44;
/// First fragment budget inside a page (`PAGE_PAYLOAD_SIZE - header`).
pub const FIRST_FRAGMENT_HEADER_ROOM: usize = 44;

/// Bit 0 of [`RecordHeader::flags`]: logical deletion event (SPEC §2.4).
pub const REC_FLAG_TOMBSTONE: u16 = 1 << 0;
/// Bit 1 of [`RecordHeader::flags`]: this record continues with raw payload
/// fragments on the following page(s).
pub const REC_FLAG_CONTINUES_ON_NEXT_PAGE: u16 = 1 << 1;

// Field offsets within the 44-byte header.
pub const RH_OFF_LSN: usize = 0;
pub const RH_OFF_TIMESTAMP: usize = 8;
pub const RH_OFF_ENTITY_ID: usize = 16;
pub const RH_OFF_PAYLOAD_SIZE: usize = 32;
pub const RH_OFF_SCHEMA_VERSION: usize = 36;
pub const RH_OFF_FLAGS: usize = 38;
pub const RH_OFF_RECORD_CRC: usize = 40;

const ZEROS4: [u8; 4] = [0; 4];

/// `#[repr(C, packed)]` exactly as written in SPEC §2.4. Field access is by
/// value only — callers never take a reference to a field of this struct, so
/// the unaligned layout never produces UB.
#[repr(C, packed)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RecordHeader {
    /// Log sequence number, globally contiguous and monotonic.
    pub lsn: u64,
    /// Unix epoch nanoseconds; strictly increasing per database.
    pub timestamp_ns: u64,
    /// 128-bit entity identifier.
    pub entity_id: u128,
    /// Payload length in bytes (whole record, including page-continuation
    /// fragments).
    pub payload_size: u32,
    /// Application-chosen payload schema version.
    pub schema_version: u16,
    /// [`REC_FLAG_TOMBSTONE`] | [`REC_FLAG_CONTINUES_ON_NEXT_PAGE`].
    pub flags: u16,
    /// CRC32-C of header (crc field zeroed) + payload.
    pub record_crc: u32,
}

const _: () = assert!(
    core::mem::size_of::<RecordHeader>() == RECORD_HEADER_SIZE,
    "RecordHeader must pack to exactly 44 bytes"
);

impl RecordHeader {
    /// Header for an in-memory record before its checksum exists.
    pub fn new(lsn: u64, timestamp_ns: u64, entity_id: u128, payload_size: usize) -> Self {
        debug_assert!(payload_size <= u32::MAX as usize);
        Self {
            lsn,
            timestamp_ns,
            entity_id,
            payload_size: payload_size as u32,
            schema_version: 0,
            flags: 0,
            record_crc: 0,
        }
    }

    pub fn payload_size(&self) -> usize {
        self.payload_size as usize
    }

    pub fn is_tombstone(&self) -> bool {
        self.flags & REC_FLAG_TOMBSTONE != 0
    }

    pub fn continues_on_next_page(&self) -> bool {
        self.flags & REC_FLAG_CONTINUES_ON_NEXT_PAGE != 0
    }

    /// Serialises with `record_crc` left zero — call
    /// [`RecordHeader::patch_record_crc`] once the checksum is known.
    pub fn encode_without_crc(&self, out: &mut [u8; RECORD_HEADER_SIZE]) {
        out.fill(0);
        LE::write_u64(&mut out[RH_OFF_LSN..], self.lsn);
        LE::write_u64(&mut out[RH_OFF_TIMESTAMP..], self.timestamp_ns);
        LE::write_u128(&mut out[RH_OFF_ENTITY_ID..], self.entity_id);
        LE::write_u32(&mut out[RH_OFF_PAYLOAD_SIZE..], self.payload_size);
        LE::write_u16(&mut out[RH_OFF_SCHEMA_VERSION..], self.schema_version);
        LE::write_u16(&mut out[RH_OFF_FLAGS..], self.flags);
        LE::write_u32(&mut out[RH_OFF_RECORD_CRC..], 0);
    }

    /// Full serialisation including the checksum field — for header images
    /// whose `record_crc` is already known.
    pub fn encode(&self, out: &mut [u8; RECORD_HEADER_SIZE]) {
        self.encode_without_crc(out);
        LE::write_u32(&mut out[RH_OFF_RECORD_CRC..], self.record_crc);
    }

    /// CRC32-C over this header (with the crc field zeroed) + `payload`.
    /// Mirror of [`RecordHeader::compute_record_crc`] that derives the
    /// header image from the struct itself.
    pub fn compute_self_crc(&self, payload: &[u8]) -> u32 {
        let mut buf = [0u8; RECORD_HEADER_SIZE];
        self.encode_without_crc(&mut buf);
        RecordHeader::compute_record_crc(&buf, payload)
    }

    /// Overwrites just the checksum field of an already-encoded buffer.
    pub fn patch_record_crc(encoded: &mut [u8; RECORD_HEADER_SIZE], crc: u32) {
        LE::write_u32(&mut encoded[RH_OFF_RECORD_CRC..], crc);
    }

    /// CRC32-C over the header image (crc zeroed) + payload, as stored on
    /// disk. One-shot form of the streaming protocol used by [`Scanner`]
    /// for records that span pages.
    ///
    /// [`Scanner`]: crate::storage::scanner::Scanner
    pub fn compute_record_crc(encoded_no_crc: &[u8; RECORD_HEADER_SIZE], payload: &[u8]) -> u32 {
        let mut h = Crc32cHasher::new();
        h.update(&encoded_no_crc[..RH_OFF_RECORD_CRC]);
        h.update(&ZEROS4);
        h.update(payload);
        h.finish()
    }

    /// Parses the 44-byte header. Slice-based, alignment-agnostic, so it is
    /// valid on any offset of a page buffer.
    pub fn decode(buf: &[u8]) -> Result<Self> {
        if buf.len() < RECORD_HEADER_SIZE {
            return Err(ChronoError::CorruptRecord {
                offset: 0,
                reason: format!(
                    "buffer holds {} bytes; a record header needs {RECORD_HEADER_SIZE}",
                    buf.len()
                ),
            });
        }
        Ok(Self {
            lsn: LE::read_u64(&buf[RH_OFF_LSN..]),
            timestamp_ns: LE::read_u64(&buf[RH_OFF_TIMESTAMP..]),
            entity_id: LE::read_u128(&buf[RH_OFF_ENTITY_ID..]),
            payload_size: LE::read_u32(&buf[RH_OFF_PAYLOAD_SIZE..]),
            schema_version: LE::read_u16(&buf[RH_OFF_SCHEMA_VERSION..]),
            flags: LE::read_u16(&buf[RH_OFF_FLAGS..]),
            record_crc: LE::read_u32(&buf[RH_OFF_RECORD_CRC..]),
        })
    }
}

/// Total on-wire size of a record with `payload_len` bytes (header + payload).
#[inline]
pub const fn record_encoded_len(payload_len: usize) -> usize {
    RECORD_HEADER_SIZE + payload_len
}

/// Zero-copy view of a record: the decoded header value plus a borrowed
/// payload slice that points directly into the caller's page buffer (for
/// records that span pages, the payload points into a reassembly buffer
/// managed by the scanner).
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct RecordRef<'a> {
    pub header: RecordHeader,
    pub payload: &'a [u8],
}

impl RecordRef<'_> {
    pub fn entity_id(&self) -> u128 {
        self.header.entity_id
    }
    pub fn record_crc(&self) -> u32 {
        self.header.record_crc
    }
    /// Verifies the record checksum in place (never copies the payload).
    pub fn verify_crc_with(&self, header_bytes: &[u8; RECORD_HEADER_SIZE]) -> bool {
        self.header.record_crc == RecordHeader::compute_record_crc(header_bytes, self.payload)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::crc::crc32c;

    fn sample() -> RecordHeader {
        RecordHeader {
            lsn: 7,
            timestamp_ns: 0x0102_0304_0506_0708,
            entity_id: 0xAABB_CCDD_EEFF_0011_2233_4455_6677_8899,
            payload_size: 3,
            schema_version: 5,
            flags: REC_FLAG_TOMBSTONE,
            record_crc: 0,
        }
    }

    #[test]
    fn header_size_and_offsets() {
        assert_eq!(core::mem::offset_of!(RecordHeader, lsn), RH_OFF_LSN);
        assert_eq!(
            core::mem::offset_of!(RecordHeader, entity_id),
            RH_OFF_ENTITY_ID
        );
        assert_eq!(
            core::mem::offset_of!(RecordHeader, record_crc),
            RH_OFF_RECORD_CRC
        );
        assert_eq!(record_encoded_len(11), 55);
    }

    #[test]
    fn wire_layout_is_little_endian() {
        let hdr = sample();
        let mut out = [0u8; RECORD_HEADER_SIZE];
        hdr.encode_without_crc(&mut out);
        assert_eq!(&out[RH_OFF_LSN..RH_OFF_LSN + 8], &7u64.to_le_bytes());
        assert_eq!(
            &out[RH_OFF_ENTITY_ID..RH_OFF_ENTITY_ID + 16],
            &hdr.entity_id.to_le_bytes()
        );
        assert_eq!(
            &out[RH_OFF_PAYLOAD_SIZE..RH_OFF_PAYLOAD_SIZE + 4],
            &3u32.to_le_bytes()
        );
        assert_eq!(
            &out[RH_OFF_FLAGS..RH_OFF_FLAGS + 2],
            &REC_FLAG_TOMBSTONE.to_le_bytes()
        );
        assert_eq!(&out[RH_OFF_RECORD_CRC..], &[0; 4]);
        let back = RecordHeader::decode(&out).unwrap();
        assert_eq!(hdr, back);
    }

    #[test]
    fn decode_rejects_short_buffer() {
        let buf = [0u8; RECORD_HEADER_SIZE - 1];
        assert!(RecordHeader::decode(&buf).is_err());
    }

    #[test]
    fn crc_convention_is_zeroed_field_then_payload() {
        let hdr = sample();
        let mut enc = [0u8; RECORD_HEADER_SIZE];
        hdr.encode_without_crc(&mut enc);
        let payload = [1u8, 2, 3];

        let fast = RecordHeader::compute_record_crc(&enc, &payload);
        // reference computation: full image with the crc field zeroed
        let mut full = enc;
        full[RH_OFF_RECORD_CRC..].fill(0);
        let mut joined = full.to_vec();
        joined.extend_from_slice(&payload);
        assert_eq!(fast, crc32c(&joined));

        RecordHeader::patch_record_crc(&mut enc, fast);
        let hdr_with_crc = RecordHeader::decode(&enc).unwrap();
        let stored_crc = hdr_with_crc.record_crc; // packed: copy by value
        assert_eq!(stored_crc, fast);
    }

    #[test]
    fn flags_helpers() {
        let mut hdr = sample();
        assert!(hdr.is_tombstone() && !hdr.continues_on_next_page());
        hdr.flags = REC_FLAG_CONTINUES_ON_NEXT_PAGE;
        assert!(!hdr.is_tombstone() && hdr.continues_on_next_page());
    }
}
