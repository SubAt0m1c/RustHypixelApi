use actix_web::web::Bytes;
use database::{cache::Database, error::ResultExt, runtime::Runtime};
use tokio::{spawn, task::spawn_blocking, time::Instant};

use crate::{cache::{cache_key::CacheKey, memory::mem_cache::MemoryCache}, error::ProcessError, logging::{log, LogMessage}};

/// Routes cache requests to the memory cache and db cache.
/// Secret requests/insertions will not query the db.
pub struct CacheRouter<RT: Runtime + Send + Sync + 'static> {
    cache: MemoryCache,
    database: Database<RT>,
}

impl<RT: Runtime + Send + Sync + 'static> CacheRouter<RT> {
    // pub fn new() -> Self {
    //     Self {
    //         cache: MemoryCache::new(),
    //         database: Database::create_new(".db"),
    //     }
    // }

    pub async fn load() -> Result<Self, ProcessError> {
        let now = Instant::now();
        let database = Database::load(".db").await?;
        log(LogMessage::TimeElapsed { elapsed: now.elapsed(), name: "Database loaded in" });
        Ok(Self { cache: MemoryCache::new(), database })
    }

    /// Attempts to get the cache entry from the cache or fetches an entry into the cache if there is none.
    pub async fn get<K: CacheKey>(&self, key: K) -> Result<Bytes, ProcessError> {
        self.cache.try_get_with(key.key(), key.get_or_insert(&self.database)).await
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

    fn spawn_blocking<T, R>(task: T) -> impl Future<Output = Result<R, database::error::Error>>
        where
            T: FnOnce() -> R + Send + 'static,
            R: Send + 'static 
    {
        spawn_blocking(task).task_err()
    }
}