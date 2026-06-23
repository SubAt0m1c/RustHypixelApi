use std::{cmp::Ordering, fmt::Display, hash::{self, Hash}, time::Duration};

use uuid::Uuid;

use crate::cache::{expires::Expires};

mod compression;
pub mod database;
pub mod cache_router;
pub mod expires;
mod memory;
pub mod cache_key;

// expires is explicitely ignored in equality and hashing.
#[derive(Eq, Clone, Copy)]
pub struct UuidKey {
    expires: Expires,
    key: u128,
}

impl PartialEq for UuidKey {
    fn eq(&self, other: &Self) -> bool {
        self.key.eq(&other.key)
    }
}

impl Hash for UuidKey {
    fn hash<H: hash::Hasher>(&self, state: &mut H) {
        self.key.hash(state);
    }
}

impl PartialOrd for UuidKey {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        self.key.partial_cmp(&other.key)
    }
}

impl Ord for UuidKey {
    fn cmp(&self, other: &Self) -> Ordering {
        self.key.cmp(&other.key)
    }
}

const FLAG_MASK: u128 = 0x0000_0000_0000_8000_C000_0000_0000_0000;
const RESTORE_MASK: u128 = 0x0000_0000_0000_0000_8000_0000_0000_0000;

impl UuidKey {
    /// uses the non-random bits in a uuid to encode a unique flag.
    /// this loses the data of the variant, but variants are basically
    /// never different in the big 26.
    pub fn encode(id: Uuid, flag: u8, expires: Expires) -> Self {
        let f = flag as u128;
        let bit2 = ((f >> 2) & 1) << 79; // flags bit 79 (unused in version)
        let bit1 = ((f >> 1) & 1) << 63; // flags bit 63 (variant bit 1)
        let bit0 = (f & 1) << 62; // flags bit 62 (variant bit 2)

        let key = (id.as_u128() & !FLAG_MASK) | bit2 | bit1 | bit0;
        Self {
            expires,
            key: key
        }
    }

    pub fn as_u128(&self) -> u128 {
        self.key
    }

    pub fn flag(&self) -> u8 {
        let encoded = self.key;
        let bit2 = ((encoded >> 79) & 1) as u8;
        let bit1 = ((encoded >> 63) & 1) as u8;
        let bit0 = ((encoded >> 62) & 1) as u8;
        (bit2 << 2) | (bit1 << 1) | bit0
    }

    /// Reconstructs a uuid from this UuidKey.
    /// This assumes the default modern RFC 4122
    /// variant. If the uuid used to construct
    /// this used a different variant, it will NOT
    /// be reconstructed properly. Do not assume
    /// accurate uuid recovery.
    pub fn uuid(&self) -> Uuid {
        Uuid::from_u128((self.key & !FLAG_MASK) | RESTORE_MASK)
    }

    pub fn expires(&self) -> Duration {
        self.expires.as_duration()
    }
}

impl Display for UuidKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "UuidKey(flag: {}, uuid: {})", self.flag(), self.uuid())
    }
}