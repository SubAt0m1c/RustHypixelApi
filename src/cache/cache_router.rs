use std::sync::LazyLock;

use actix_web::web::Bytes;
use ltmdb::{ResultExt, Runtime};
use pingora_memory_cache::MemoryCache;
use rapidhash_lite::RandomHash;
use single_flight::Group;
use tokio::{spawn, task::spawn_blocking, time::{Instant, sleep}};

use crate::{cache::{UuidKey, cache_key::CacheKey}, env_var, error::ProcessError, logging::{LogMessage, log}, routes::stats::RateLimit};

static CACHE_SIZE: LazyLock<usize> = LazyLock::new(|| env_var("CACHE_SIZE", 256));


pub type Database = ltmdb::Database<TokioRT, RandomHash>;

/// Routes cache requests to the memory cache and db cache.
/// behavior during insertion is handled via the `CacheKey` trait.
pub struct CacheRouter {
    cache: MemoryCache<UuidKey, Bytes>,
    database: Database,
    group: Group<UuidKey, Bytes, ProcessError, RandomHash>,
}

impl CacheRouter {
    pub async fn load() -> Result<Self, ProcessError> {
        let now = Instant::now();
        let database = Database::load(".db").await?;
        log(LogMessage::TimeElapsed { elapsed: now.elapsed(), name: "database load" });
        Ok(Self { cache: MemoryCache::new(*CACHE_SIZE), database, group: Group::with_hasher(RandomHash::default()) })
    }

    /// Attempts to get the cache entry from the cache or fetches an entry into the cache if there is none.
    pub async fn get<K: CacheKey>(&self, key: K, rate_limit: &RateLimit) -> Result<Bytes, ProcessError> {
        let k = key.key();

        // we check the cache outside the singleflight group, since it's much more expensive to start that work 
        // just to check the cache if its already there and doesn't need suppression
        if let (Some(entry), _status) = self.cache.get(&k) {
            return Ok(entry);
        }
        
        // singleflight coelesces the key.get_or_insert requests so we dont duplicate work on quick duplicate requests
        let res = self.group.work(&k, async move {
            // we check again here since it may have been added between the prior call and when the group started the work.
            if let (Some(entry), _status) = self.cache.get(&k) { 
                return Ok(entry);
            }
            
            let (data, ttl) = key.get_or_insert(&self.database, rate_limit).await?;
            self.cache.put(&k, data.clone(), Some(ttl)); // store the result in the cache BEFORE the end of duplicate suppression
            Ok(data)
        }).await;

        res.map_err(|err| err.unwrap_or(ProcessError::InternalServer("Single Flight Leader failed!")))
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