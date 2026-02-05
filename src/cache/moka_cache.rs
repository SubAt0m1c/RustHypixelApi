use crate::cache::compression::{compress_data, extract_data};
use actix_web::web::Bytes;
use futures::StreamExt;
use futures::stream::FuturesUnordered;
use moka::future::Cache;
use moka::Expiry;
use serde::Serialize;
use uuid::Uuid;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

const CACHE_SIZE: u64 = 256;

// arced because moka needs to clone when it gets entries.
pub type MokaEntry = Arc<ExpireEntry>;

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
    pub fn new(duration: Duration, value: Bytes) -> MokaEntry {
        Arc::new(Self { duration, value })
    }
}

pub struct Expire;
impl Expiry<MokaKey, MokaEntry> for Expire {
    fn expire_after_create(
        &self,
        _key: &MokaKey,
        value: &MokaEntry,
        _created_at: Instant,
    ) -> Option<Duration> {
        Some(value.duration)
    }
}

#[derive(Clone)]
pub struct MokaCache(Cache<MokaKey, MokaEntry>);

impl MokaCache {
    pub fn new() -> Self {
        let cache = Cache::builder()
            .max_capacity(CACHE_SIZE)
            .expire_after(Expire)
            .build();

        Self(cache)
    }
    
    pub async fn insert(&self, key: MokaKey, value: Bytes, duration: Duration) {
        self.0
            .insert(key, ExpireEntry::new(duration, compress_data(&value).into()))
            .await
    }

    pub async fn get(&self, key: MokaKey) -> Option<Bytes> {
        let extracted = extract_data(&self.0.get(&key).await?.value).ok()?;
        Some(Bytes::from(extracted))
    }
}
