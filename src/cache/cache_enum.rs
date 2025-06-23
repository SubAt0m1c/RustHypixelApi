use crate::cache::moka_cache::MokaCache;
use actix_web::web::Bytes;
use std::time::Duration;

/// this is mainly for experimenting.
#[derive(Clone)]
pub enum CacheEnum {
    // LRU(Arc<Mutex<LRCache>>),
    MOKA(MokaCache),
}

impl CacheEnum {
    pub async fn insert(&self, key: String, json: Bytes, duration: Duration) {
        match self {
            // CacheEnum::LRU(val) => {
            //     let mut locked = val.lock().unwrap();
            //     locked.insert(key, json);
            // }
            CacheEnum::MOKA(val) => val.insert(key, json, duration).await.unwrap(),
        }
    }

    pub async fn get(&self, key: &str, expire: Duration) -> Option<String> {
        match self {
            // CacheEnum::LRU(val) => {
            //     let mut locked = val.lock().unwrap();
            //     locked.get(key, chrono::Duration::from_std(expire).unwrap())
            // }
            CacheEnum::MOKA(val) => val.get(key).await,
        }
    }

    pub async fn cache_raw(&self, cache_key: String, data: Bytes, duration: Duration) {
        self.insert(cache_key, data, duration).await;
    }
}
