use actix_web::web::Bytes;
use ltmdb::{Database, ResultExt, Runtime};
use tokio::{spawn, task::spawn_blocking, time::Instant};

use crate::{cache::{cache_key::CacheKey, memory::{CacheEntry, MemoryCache}}, error::ProcessError, logging::{LogMessage, log}};

/// Routes cache requests to the memory cache and db cache.
/// behavior during insertion is handled via the `CacheKey` trait.
pub struct CacheRouter {
    cache: MemoryCache,
    database: Database<TokioRT>,
}

impl CacheRouter {
    pub async fn load() -> Result<Self, ProcessError> {
        let now = Instant::now();
        let database = Database::load(".db").await?;
        log(LogMessage::TimeElapsed { elapsed: now.elapsed(), name: "database load" });
        Ok(Self { cache: MemoryCache::new(), database })
    }

    /// Attempts to get the cache entry from the cache or fetches an entry into the cache if there is none.
    pub async fn get<K: CacheKey>(&self, key: K) -> Result<Bytes, ProcessError> {
        self.cache.try_get_with(key.key(), key.get_or_insert(&self.database)).await.map(CacheEntry::into_bytes)
    }
}

pub struct TokioRT;
impl Runtime for TokioRT {
    fn spawn<T>(task: T)
        where
            T: Future + Send + 'static,
            T::Output: Send + 'static 
    {
        spawn(task);
    }

    fn spawn_blocking<T, R>(task: T) -> impl Future<Output = Result<R, ltmdb::Error>>
        where
            T: FnOnce() -> R + Send + 'static,
            R: Send + 'static 
    {
        spawn_blocking(task).task_err()
    }
}