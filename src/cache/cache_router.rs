use std::sync::Arc;

use actix_web::web::Bytes;
use ltmdb::{ResultExt, Runtime};
use rapidhash_lite::RandomHash;
use single_flight::Group;
use tokio::{spawn, task::spawn_blocking, time::{Instant, sleep}};

use crate::{cache::{UuidKey, cache_key::CacheKey, memory::{CacheEntry, MemoryCache}}, error::ProcessError, logging::{LogMessage, log}, routes::stats::RateLimit};

pub type Database = ltmdb::Database<TokioRT, RandomHash>;

/// Routes cache requests to the memory cache and db cache.
/// behavior during insertion is handled via the `CacheKey` trait.
pub struct CacheRouter {
    cache: MemoryCache,
    database: Database,
    group: Group<UuidKey, CacheEntry, Arc<ProcessError>, RandomHash>
}

impl CacheRouter {
    pub async fn load() -> Result<Self, ProcessError> {
        let now = Instant::now();
        let database = Database::load(".db").await?;
        log(LogMessage::TimeElapsed { elapsed: now.elapsed(), name: "database load" });
        Ok(Self { cache: MemoryCache::new(), database, group: Group::with_hasher(RandomHash::default()) })
    }

    /// Attempts to get the cache entry from the cache or fetches an entry into the cache if there is none.
    pub async fn get<K: CacheKey>(&self, key: K, rate_limit: &RateLimit) -> Result<Bytes, ProcessError> {
        let k = key.key();

        let res = self.group.work(&k, async move {
            // moka cache says it basically has in flight deduplication but we do our own.
            self.cache.try_get_with(k, key.get_or_insert(&self.database, rate_limit)).await
        }).await;

        match res {
            Ok(entry) => Ok(entry.into_bytes()),
            Err(Some(e)) => Err(Arc::unwrap_or_clone(e)),
            Err(None) => Err(ProcessError::InternalServer("Single Flight Leader returned an error.")),
        }
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

    fn sleep(duration: std::time::Duration) -> impl Future<Output = ()> {
        sleep(duration)
    }
}