//! On-disk binary format, low-level disk I/O, the append-only page packer
//! and the crash-safety surface of ChronoDB.
//!
//! Everything in this module speaks little-endian byte layouts exactly as
//! defined by `SPECIFICATION.md` section 2.
//!
//! Target platforms: POSIX kernels — Linux x86_64 / aarch64. Anything else
//! fails at compile time rather than silently degrading.

#[cfg(not(unix))]
compile_error!("chronodb storage targets POSIX (Linux x86_64 / aarch64); unix build required");

pub mod crc;
pub mod file;
pub mod page;
pub mod record;
pub mod scanner;
pub mod writer;
