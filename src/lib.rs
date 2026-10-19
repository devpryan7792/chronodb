//! ChronoDB crate root: storage (on-disk format, I/O, packer), index (mmap
//! + sparse index), and engine (concurrency, facade).
#![cfg_attr(docsrs, feature(doc_cfg))]

pub mod engine;
pub mod error;
pub mod index;
pub mod storage;
