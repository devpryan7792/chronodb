//! Prefix-free error and result types for every ChronoDB failure mode.

use std::io;
use thiserror::Error;

/// Convenience alias used throughout the crate.
pub type Result<T, E = ChronoError> = std::result::Result<T, E>;

/// Every failure mode the storage engine can surface.
#[derive(Debug, Error)]
pub enum ChronoError {
    #[error("i/o error: {0}")]
    Io(#[from] io::Error),

    #[error("corrupt superblock: {reason}")]
    CorruptSuperblock { reason: String },

    #[error("corrupt page {page_index}: {reason}")]
    CorruptPage { page_index: u64, reason: String },

    #[error(
        "page {page_index} failed crc32c verify: stored {stored:#010x}, computed {computed:#010x}"
    )]
    PageChecksumMismatch {
        page_index: u64,
        stored: u32,
        computed: u32,
    },

    #[error("record {lsn} failed crc32c verify")]
    RecordChecksumMismatch { lsn: u64 },

    #[error("corrupt record at byte offset {offset}: {reason}")]
    CorruptRecord { offset: u64, reason: String },

    #[error("non-monotonic lsn: previous {previous}, invalid successor {current}")]
    NonMonotonicLsn { previous: u64, current: u64 },

    #[error("non-monotonic timestamp: previous {previous}, rejected {current}")]
    NonMonotonicTimestamp { previous: u64, current: u64 },

    #[error("payload too large: {size} bytes exceeds the engine limit of {max} bytes")]
    RecordTooLarge { size: usize, max: usize },

    #[error("engine closed: no further appends accepted")]
    EngineClosed,

    #[error("write worker failed: {0}")]
    WriterFailed(String),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn errors_format_sanely() {
        let e = ChronoError::PageChecksumMismatch {
            page_index: 7,
            stored: 0xDEADBEEF,
            computed: 0x0000_FFFF,
        };
        let s = e.to_string();
        assert!(s.contains("page 7") && s.contains("0xdeadbeef") && s.contains("0x0000ffff"));
    }
}
