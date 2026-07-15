//! reimplementation of rapidhash nano from [GitHub](https://github.com/Nicoshev/rapidhash/blob/master/rapidhash.h#L432) under MIT license.
//! (Nano because hashing more than 48 bytes is hardly expected)

#![allow(clippy::inline_always, clippy::unreadable_literal)]

use std::{hash::{BuildHasher, Hasher, RandomState}, hint::cold_path, sync::LazyLock};

// constants used by the original hasher, corresponding to the secrets at the named index.
const RH0: u64 = 0x2d358dccaa6c78a5;
const RH1: u64 = 0x8bb84b93962eacc9;
const RH2: u64 = 0x4b33a62ed433d4a3;
const RH7: u64 = 0xaaaaaaaaaaaaaaaa;

#[derive(Clone, Copy, Debug)]
pub struct RapidHash {
    state: u64,
}

impl Hasher for RapidHash {
    #[inline(always)]
    fn finish(&self) -> u64 {
        self.state
    }

    #[inline(always)]
    fn write(&mut self, bytes: &[u8]) {
        self.state = rapidhash_nano(self.state, bytes);
    }

    // Direct u64 write since we use them for bucket keys and this is branchless.
    // Could/Should be implemented for the others if we switch to generic keys.
    #[inline(always)]
    fn write_u64(&mut self, i: u64) {
        self.state ^= mix(self.state ^ RH2, RH1);
        self.state ^= size_of::<u64>() as u64;
        self.state = finish(i, i, self.state, 8);
    }
}

#[inline(always)]
#[allow(clippy::similar_names)]
pub const fn rapidhash_nano(mut seed: u64, bytes: &[u8]) -> u64 {
    seed ^= mix(seed ^ RH2, RH1);
    
    let mut a = 0;
    let mut b = 0;

    let remaining;
    if likely(bytes.len() <= 16) {
        if bytes.len() >= 4 {
            seed ^= bytes.len() as u64;
            if bytes.len() >= 8 {
                a = read_u64(bytes, 0);
                b = read_u64(bytes, bytes.len() - 8);
            } else {
                a = read_u32(bytes, 0) as u64;
                b = read_u32(bytes, bytes.len() - 4) as u64;
            }
        } else if !bytes.is_empty() {
            a = ((bytes[0] as u64) << 45) | bytes[bytes.len() - 1] as u64;
            b = bytes[bytes.len() >> 1] as u64;
        }
        remaining = bytes.len();
    } else {
        let mut slice = bytes;
        if slice.len() > 48 {
            let mut see1 = seed;
            let mut see2 = seed;

            while slice.len() > 48 {
                seed = mix(read_u64(slice, 0) ^ RH0, read_u64(slice, 8) ^ seed);
                see1 = mix(read_u64(slice, 16) ^ RH1, read_u64(slice, 24) ^ see1);
                see2 = mix(read_u64(slice, 32) ^ RH2, read_u64(slice, 40) ^ see2);
                slice = slice.split_at(48).1;
            }

            seed ^= see1;
            seed ^= see2;
        }

        if slice.len() > 16 {
            seed = mix(read_u64(slice, 0) ^ RH2, read_u64(slice, 8) ^ seed);
            if slice.len() > 32 {
                seed = mix(read_u64(slice, 16) ^ RH2, read_u64(slice, 24) ^ seed);
            }
        }

        a = read_u64(bytes, bytes.len() - 16) ^ slice.len() as u64;
        b = read_u64(bytes, bytes.len() - 8);
        remaining = slice.len();
    }

    finish(a, b, seed, remaining)
}

#[inline(always)]
const fn finish(mut a: u64, mut b: u64, seed: u64, remaining: usize) -> u64 {
    (a, b) = mum(a ^ RH2, b ^ seed);
    mix(a ^ RH7, b ^ RH1 ^ remaining as u64)
}

#[inline(always)]
const fn read_u64(slice: &[u8], offset: usize) -> u64 {
    u64::from_le_bytes(*slice.split_at(offset).1.first_chunk::<8>().expect("Should have verified theres more than 8 bytes left in slice."))
}

#[inline(always)]
const fn read_u32(slice: &[u8], offset: usize) -> u32 {
    u32::from_le_bytes(*slice.split_at(offset).1.first_chunk::<4>().expect("Should have verified theres more than 4 bytes left in slice."))
}

#[inline(always)]
#[allow(clippy::cast_possible_truncation)]
const fn mum(a: u64, b: u64) -> (u64, u64) {
    let r = (a as u128).wrapping_mul(b as u128);

    (r as u64, (r >> 64) as u64)
}

#[inline(always)]
#[allow(clippy::cast_possible_truncation)]
const fn mix(a: u64, b: u64) -> u64 {
    let r = (a as u128).wrapping_mul(b as u128);

    (r as u64) ^ (r >> 64) as u64
}


impl Default for RapidHash {
    fn default() -> Self {
        // May help against trivial attempts at hashdos attacks. RapidHash is not designed to be cryptographically secure.
        static DEFAULT_SEED: LazyLock<u64> = LazyLock::new(|| RandomState::new().build_hasher().finish()); // just piggyback of the default randomness.
        Self {
            state: *DEFAULT_SEED
        }
    }
}

impl BuildHasher for RapidHash {
    type Hasher = Self;

    fn build_hasher(&self) -> Self::Hasher {
        Self::default()
    }
}

#[inline(always)]
const fn likely(b: bool) -> bool {
    if !b {
        cold_path();
    }
    b
}