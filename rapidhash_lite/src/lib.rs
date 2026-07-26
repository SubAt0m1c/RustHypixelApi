//! Light reimplementation of [RapidHash Nano](https://github.com/Nicoshev/rapidhash/blob/master/rapidhash.h#L432) under MIT license.
//! 
//! Not designed to be cryptographically secure.
//! Use `RandomHash` for randomized hashing for use in hashmasp where you don't need determinism.
//! 
//! (Nano because hashing more than 48 bytes is hardly expected)

#![allow(clippy::inline_always, clippy::unreadable_literal)]

use std::{hash::{BuildHasher, Hasher}, hint::cold_path};

// constants used by the original hasher, corresponding to the secrets at the named index.
const RH0: u64 = 0x2d358dccaa6c78a5;
const RH1: u64 = 0x8bb84b93962eacc9;
const RH2: u64 = 0x4b33a62ed433d4a3;
const RH7: u64 = 0xaaaaaaaaaaaaaaaa;

/// A Fast, non-cryptographic hash function.
/// Use `RandomHash` for randomized hashing for use in non-deterministic hashmaps.
/// 
/// Uses the [RapidHash Nano](https://github.com/Nicoshev/rapidhash/blob/master/rapidhash.h#L432) algorithm.
#[derive(Clone, Copy, Debug)]
pub struct RapidHash {
    state: u64,
}

impl RapidHash {
    /// Creats a new `RapidHash` with the default seed.
    #[must_use]
    pub fn new() -> Self {
        Self {
            state: 0,
        }
    }

    /// Creates a new `RapidHash` with the given seed.
    #[must_use]
    pub fn with_seed(seed: u64) -> Self {
        Self {
            state: seed,
        }
    }

    /// Hashes the given bytes using the current state, returning the hashed value.
    /// This does not mutate state.
    #[must_use]
    pub fn hash(&self, bytes: &[u8]) -> u64 {
        rapidhash_nano(self.state, bytes)
    }
}

impl Hasher for RapidHash {
    #[inline(always)]
    fn finish(&self) -> u64 {
        self.state
    }

    #[inline(always)]
    fn write(&mut self, bytes: &[u8]) {
        self.state = self.hash(bytes);
    }
    
    #[inline(always)]
    fn write_u8(&mut self, i: u8) {
        self.state ^= mix(self.state ^ RH2, RH1);
        self.state ^= size_of::<u8>() as u64;
        let i = u64::from(i);
        self.state = finish((i << 45) | i, i, self.state, size_of::<u8>());
    }

    #[inline(always)]
    fn write_u16(&mut self, i: u16) {
        self.state ^= mix(self.state ^ RH2, RH1);
        self.state ^= size_of::<u16>() as u64;
        let hi = u64::from(i >> 8);
        let lo = u64::from(i & 0xff); 
        self.state = finish((hi << 45) | lo, hi, self.state, size_of::<u16>());
    }

    #[inline(always)]
    fn write_u32(&mut self, i: u32) {
        self.state ^= mix(self.state ^ RH2, RH1);
        self.state ^= size_of::<u32>() as u64;
        let hi = u64::from(i >> 16);
        let lo = u64::from(i & 0xffff); 
        self.state = finish(hi, lo, self.state, size_of::<u32>());
    }

    #[inline(always)]
    fn write_usize(&mut self, i: usize) {
        #[cfg(target_pointer_width = "32")]
        self.write_u32(i as u32);
        #[cfg(target_pointer_width = "64")]
        self.write_u64(i as u64);
    }

    #[inline(always)]
    fn write_u64(&mut self, i: u64) {
        self.state ^= mix(self.state ^ RH2, RH1);
        self.state ^= size_of::<u64>() as u64;
        self.state = finish(i, i, self.state, size_of::<u64>());
    }

    #[inline(always)]
    fn write_u128(&mut self, i: u128) {
        self.state ^= mix(self.state ^ RH2, RH1);
        self.state ^= size_of::<u128>() as u64;
        let hi = (i >> 64) as u64;
        let lo = (i & 0xffff_ffff_ffff_ffff) as u64; 
        self.state = finish(hi, lo, self.state, size_of::<u128>());
    }
}

#[must_use]
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
        Self::new()
    }
}

impl BuildHasher for RapidHash {
    type Hasher = Self;

    fn build_hasher(&self) -> Self::Hasher {
        Self::default()
    }
}

/// Random generator for `RapidHash`.
#[derive(Clone)]
pub struct RandomHash {
    random_state: u64,
}

impl Default for RandomHash {
    // Copied from (fastrand)[https://github.com/smol-rs/fastrand/blob/master/src/global_rng.rs#L203] under MIT.
    // Generates a sufficiently random initial hasher seed.
    fn default() -> Self {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};
        use std::thread;
        use std::time::Instant;
        
        let mut hasher = DefaultHasher::new();
        Instant::now().hash(&mut hasher);
        thread::current().id().hash(&mut hasher);
        Self { random_state: hasher.finish() }
    }
}

impl BuildHasher for RandomHash {
    type Hasher = RapidHash;

    fn build_hasher(&self) -> Self::Hasher {
        RapidHash::with_seed(self.random_state)
    }
}

#[inline(always)]
const fn likely(b: bool) -> bool {
    if !b {
        cold_path();
    }
    b
}