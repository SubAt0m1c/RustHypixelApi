use crate::cache::compression::{compress_data, extract_data};
use actix_web::web::Bytes;
use moka::future::Cache;
use moka::Expiry;
use serde::Serialize;
use uuid::Uuid;
use std::env;
use std::sync::{Arc, LazyLock};
use std::time::{Duration, Instant};

static CACHE_SIZE: LazyLock<u64> = LazyLock::new(|| {
    let size = env::var("CACHE_SIZE");
    match size {
        Ok(size) => {
            size.parse().expect("CACHE_SIZE should be a !")
        }
        Err(e) => {
            eprintln!("Couldn't find environment variable for CACHE_SIZE, using 256 default. {e}");
            256u64
        }
    }
});

#[derive(Hash, PartialEq, Eq, PartialOrd, Ord, Clone, Copy, Serialize)]
pub enum MokaKey {
    Secrets(Uuid),
    Profile(Uuid),
}

impl MokaKey {
    pub fn secrets(key: Uuid) -> MokaKey {
        MokaKey::Secrets(key)
    }
    
    pub fn profile(key: Uuid) -> MokaKey {
        MokaKey::Profile(key)
    }
    
    pub fn uuid(&self) -> Uuid {
        match self {
            Self::Profile(id) => *id,
            Self::Secrets(id) => *id,
        }
    }
}

#[derive(Clone)]
pub struct ExpireEntry {
    duration: Duration,
    value: Bytes,
}

impl ExpireEntry {
    pub fn new(duration: Duration, value: Bytes) -> Self {
        Self { duration, value }
    }
}

pub struct Expire;
impl Expiry<MokaKey, ExpireEntry> for Expire {
    fn expire_after_create(
        &self,
        _key: &MokaKey,
        value: &ExpireEntry,
        _created_at: Instant,
    ) -> Option<Duration> {
        Some(value.duration)
    }
}

#[derive(Clone)]
pub struct MokaCache(Cache<MokaKey, ExpireEntry>);

impl MokaCache {
    pub fn new() -> Self {
        let cache = Cache::builder()
            .max_capacity(*CACHE_SIZE)
            .expire_after(Expire)
            .build();

        Self(cache)
    }

    pub async fn get_or_insert_with<F>(&self, key: MokaKey, init: F) -> Option<Bytes>
    where
            F: Future<Output = Option<ExpireEntry>>,
    {
        self.0.optionally_get_with(key, init).await.map(|e| e.value)
    }
    
    pub async fn insert(&self, key: MokaKey, value: Bytes, duration: Duration) {
        self.0
            .insert(key, ExpireEntry::new(duration, value))
            .await
    }

    pub async fn get(&self, key: MokaKey) -> Option<Bytes> {
        Some(self.0.get(&key).await?.value)
    }
}
