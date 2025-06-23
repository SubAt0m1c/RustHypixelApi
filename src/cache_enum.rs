use crate::lru_cache::LRCache;
use crate::moka_cache::MokaCache;
use serde_json::Value;
use std::sync::{Arc, Mutex};
use std::time::Duration;

/// this is mainly for experimenting.
#[derive(Clone)]
pub enum CacheEnum {
    LRU(Arc<Mutex<LRCache>>),
    MOKA(MokaCache),
}

impl CacheEnum {
    pub async fn insert(&self, key: String, json: &Value, duration: Duration) {
        match self { 
            CacheEnum::LRU(val) => {
                let mut locked = val.lock().unwrap();
                locked.insert(key, json);
            }
            CacheEnum::MOKA(val) => {
                val.insert(key, json, duration).await.unwrap()
            }
        }
    }
    
    pub async fn get(&self, key: &str, expire: Duration) -> Option<Value> {
        match self { 
            CacheEnum::LRU(val) => {
                let mut locked = val.lock().unwrap();
                locked.get(key, chrono::Duration::from_std(expire).unwrap())
            }
            CacheEnum::MOKA(val) => {
                val.get(key).await
            }
        }
    }
}