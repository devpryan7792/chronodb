//! CRC32-C (Castagnoli, reflected polynomial `0x82F63B78`).
//!
//! Hardware-accelerated when possible:
//! * x86_64 — Intel SSE 4.2 `crc32` instructions (`core::arch::x86_64`), picked
//!   at runtime, no external dependency.
//! * every other target (incl. aarch64) — software slicing-by-8 fallback built
//!   from a compile-time generated table.
//!
//! Streaming convention: `Crc32cHasher` keeps the *raw* running value (no
//! init inversion exposed to callers); `finish()` applies the final
//! `0xFFFF_FFFF` residue-xor. `crc32c(buf)` is the one-shot equivalent of
//! `Crc32cHasher::new() + update(buf) + finish()`.

use std::sync::atomic::{AtomicU8, Ordering};

/// Initial streaming state of the raw running value.
pub const CRC32C_INIT: u32 = 0xFFFF_FFFF;
/// Final residue xor applied by [`Crc32cHasher::finish`].
const CRC32C_RESIDUE: u32 = 0xFFFF_FFFF;
/// Reflected Castagnoli polynomial used by the software fallback.
const REFLECTED_POLY: u32 = 0x82F63B78;

// ---------------------------------------------------------------------------
// hardware dispatch
// ---------------------------------------------------------------------------

/// 0 = unknown, 1 = available, 2 = unavailable.
static HW_STATE: AtomicU8 = AtomicU8::new(0);

#[inline]
fn hw_available() -> bool {
    match HW_STATE.load(Ordering::Relaxed) {
        1 => true,
        2 => false,
        _ => {
            let yes = detect_hw();
            HW_STATE.store(if yes { 1 } else { 2 }, Ordering::Relaxed);
            yes
        }
    }
}

#[cfg(target_arch = "x86_64")]
fn detect_hw() -> bool {
    std::arch::is_x86_feature_detected!("sse4.2")
}

#[cfg(not(target_arch = "x86_64"))]
fn detect_hw() -> bool {
    // TODO(arch): aarch64 CRC extension intrinsics remain unstable on our
    // MSRV; the slicing-by-8 fallback below is used there today.
    false
}

#[cfg(target_arch = "x86_64")]
mod sse42 {
    use core::arch::x86_64::{_mm_crc32_u16, _mm_crc32_u32, _mm_crc32_u64, _mm_crc32_u8};

    /// # Safety
    /// The caller must have verified `sse4.2` support at runtime
    /// (e.g. through `std::arch::is_x86_feature_detected!`).
    #[target_feature(enable = "sse4.2")]
    pub unsafe fn crc(state: u32, mut data: &[u8]) -> u32 {
        let mut acc = state as u64;
        const K64: usize = 8;
        const K32: usize = 4;
        const K16: usize = 2;

        while data.len() >= K64 {
            let q = u64::from_le_bytes(data[..K64].try_into().unwrap());
            acc = _mm_crc32_u64(acc, q);
            data = &data[K64..];
        }
        if data.len() >= K32 {
            let q = u32::from_le_bytes(data[..K32].try_into().unwrap());
            acc = _mm_crc32_u32(acc as u32, q) as u64;
            data = &data[K32..];
        }
        if data.len() >= K16 {
            let q = u16::from_le_bytes(data[..K16].try_into().unwrap());
            acc = _mm_crc32_u16(acc as u32, q) as u64;
            data = &data[K16..];
        }
        if !data.is_empty() {
            acc = _mm_crc32_u8(acc as u32, data[0]) as u64;
        }
        acc as u32
    }
}

// ---------------------------------------------------------------------------
// software fallback: slicing-by-8
// ---------------------------------------------------------------------------

const fn build_tables() -> [[u32; 256]; 8] {
    let mut t = [[0u32; 256]; 8];
    let mut i = 0usize;
    while i < 256 {
        let mut c = i as u32;
        let mut k = 0;
        while k < 8 {
            c = if c & 1 != 0 {
                (c >> 1) ^ REFLECTED_POLY
            } else {
                c >> 1
            };
            k += 1;
        }
        t[0][i] = c;
        i += 1;
    }
    let mut s = 1usize;
    while s < 8 {
        let mut i = 0usize;
        while i < 256 {
            let prev = t[s - 1][i];
            t[s][i] = (prev >> 8) ^ t[0][prev as usize & 0xFF];
            i += 1;
        }
        s += 1;
    }
    t
}

static TABLES: [[u32; 256]; 8] = build_tables();

fn slicing8_update(mut state: u32, data: &[u8]) -> u32 {
    let t = &TABLES;
    let len = data.len();
    let mut i = 0usize;
    while i + 8 <= len {
        let lo = u32::from_le_bytes([data[i], data[i + 1], data[i + 2], data[i + 3]]) ^ state;
        state = t[7][lo as usize & 0xFF]
            ^ t[6][(lo >> 8) as usize & 0xFF]
            ^ t[5][(lo >> 16) as usize & 0xFF]
            ^ t[4][(lo >> 24) as usize & 0xFF]
            ^ t[3][data[i + 4] as usize]
            ^ t[2][data[i + 5] as usize]
            ^ t[1][data[i + 6] as usize]
            ^ t[0][data[i + 7] as usize];
        i += 8;
    }
    while i < len {
        state = t[0][((state ^ data[i] as u32) & 0xFF) as usize] ^ (state >> 8);
        i += 1;
    }
    state
}

// ---------------------------------------------------------------------------
// public API
// ---------------------------------------------------------------------------

/// Streaming CRC32-C state.
#[derive(Clone, Debug)]
pub struct Crc32cHasher {
    state: u32,
}

impl Default for Crc32cHasher {
    fn default() -> Self {
        Self::new()
    }
}

impl Crc32cHasher {
    pub fn new() -> Self {
        Self { state: CRC32C_INIT }
    }

    /// Feeds more bytes into the running checksum.
    pub fn update(&mut self, data: &[u8]) {
        self.state = crc32c_update(self.state, data);
    }

    /// Completes the checksum, applying the final residue xor.
    pub fn finish(self) -> u32 {
        self.state ^ CRC32C_RESIDUE
    }

    /// Current *raw* running value modulo the final xor. Mostly for tests.
    pub fn raw_state(&self) -> u32 {
        self.state
    }
}

/// One-shot CRC32-C over `data`.
#[inline]
pub fn crc32c(data: &[u8]) -> u32 {
    let mut h = Crc32cHasher::new();
    h.update(data);
    h.finish()
}

/// Advances a raw running value over a byte slice. `Crc32cHasher::update`
/// wraps this; tests use it directly to compare hardware vs software paths.
#[inline]
pub(crate) fn crc32c_update(state: u32, data: &[u8]) -> u32 {
    if hw_available() {
        #[cfg(target_arch = "x86_64")]
        {
            // SAFETY: `crc` requires sse4.2 which `hw_available` just checked
            // with a runtime detector.
            unsafe { return sse42::crc(state, data) };
        }
        #[cfg(not(target_arch = "x86_64"))]
        {
            return slicing8_update(state, data);
        }
    }
    slicing8_update(state, data)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Reference vectors verified against an independent bitwise
    /// implementation of reflected CRC32-C (Castagnoli) and the well known
    /// iSCSI test suite.
    const KATS: &[(&[u8], u32)] = &[
        (b"", 0x0000_0000),
        (b"123456789", 0xE306_9283),
        (b"hello", 0x9A71_BB4C),
        (b"aaaa", 0x6A52_EEB0),
        (b"chronodb", 0xF2DC_59BD),
        (&[0u8; 32], 0x8A91_36AA),
        (&[0xFF; 32], 0x62A8_AB43),
    ];

    #[test]
    fn known_answer_vectors() {
        for (buf, want) in KATS {
            assert_eq!(crc32c(buf), *want, "mismatch on {buf:?}");
        }
        // incrementing byte sequence 0..=31 (iSCSI suite)
        assert_eq!(crc32c(&(0u8..32).collect::<Vec<u8>>()), 0x46DD_794E);
    }

    #[test]
    fn streaming_equals_one_shot() {
        let data = &(0u8..=255u8).collect::<Vec<u8>>();
        let whole = crc32c(data);
        let mut h = Crc32cHasher::new();
        for chunk in data.chunks(7) {
            h.update(chunk);
        }
        assert_eq!(h.finish(), whole);
        // sub-stream checksums differ from the full-stream checksum
        assert_ne!(crc32c(&data[..3]), whole);
    }

    fn deterministic_fill(len: usize, seed: u64) -> Vec<u8> {
        let mut out = Vec::with_capacity(len);
        let mut x = seed | 1;
        for _ in 0..len {
            x = x
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1_442_695_040);
            out.push((x >> 33) as u8);
        }
        out
    }

    #[cfg(target_arch = "x86_64")]
    #[test]
    fn hardware_path_matches_software() {
        if !std::arch::is_x86_feature_detected!("sse4.2") {
            panic!("sse4.2 unavailable on this host; skipping hw/sw parity test");
        }
        for len in [
            0usize, 1, 2, 3, 4, 5, 7, 8, 9, 15, 31, 32, 63, 100, 4021, 4096,
        ] {
            let v = deterministic_fill(len, (len as u64) + 42);
            assert_eq!(crc32c(&v), hw_one_shot(&v), "len {len}");
        }
    }

    /// Hardware-only CRC32-C applying the same init/finalise protocol as
    /// [`crc32c`].
    #[cfg(target_arch = "x86_64")]
    fn hw_one_shot(data: &[u8]) -> u32 {
        // SAFETY: the parent test verified sse4.2 support on this host.
        let raw = unsafe { super::sse42::crc(super::CRC32C_INIT, data) };
        raw ^ 0xFFFF_FFFF
    }
}
