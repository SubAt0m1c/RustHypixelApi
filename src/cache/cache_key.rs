use std::time::Duration;

use serde::Serialize;
use uuid::Uuid;

use crate::routes::{profile::PROFILE_CACHE_TTL, secrets::SECRETS_TTL_SECONDS};

/// Key used for both DB cache and memory caches.
#[derive(Hash, PartialEq, Eq, PartialOrd, Ord, Clone, Copy, Serialize, Debug)]
pub enum CacheKey {
    Secrets(Uuid),
    Profile(Uuid),
}

impl CacheKey {
    pub fn hypixel_url(&self) -> String {
        match self {
            CacheKey::Profile(id) => format!("https://api.hypixel.net/v2/skyblock/profiles?uuid={}", id),
            CacheKey::Secrets(id) => format!("https://api.hypixel.net/v2/player?uuid={}", id),
        }
    }

    pub fn cache_ttl(&self) -> Duration {
        Duration::from_secs(match self {
            CacheKey::Profile(_) => *PROFILE_CACHE_TTL,
            CacheKey::Secrets(_) => *SECRETS_TTL_SECONDS,
        })
    }
}