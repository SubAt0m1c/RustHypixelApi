use std::{sync::{Arc, LazyLock}, time::Instant};

use actix_web::web::Bytes;
use dashmap::{DashMap, Entry};
use futures::{FutureExt, future::{BoxFuture, Shared}};
use reqwest::{header:: HeaderMap, Client};

use crate::{API_KEY, cache::{cache_key::CacheKey, cache_router::CacheRouter}, error::ProcessError, logging::{LogMessage, log}};
type Pending = Shared<BoxFuture<'static, Result<Bytes, ProcessError>>>;

static CLIENT: LazyLock<Client> = LazyLock::new(|| {
    let api_key = API_KEY.get().expect("Api key should have been set already!");

    let mut headers = HeaderMap::new();
    headers.insert("API-Key", api_key.parse().unwrap());
    
    Client::builder()
        .default_headers(headers.clone())
        .build()
        .unwrap()
});

/// Handles api requests by querying a CacheRouter and then querying the upstream hypixel API.
/// duplicate concurrent upstream requests are put into a pending queue and only query upstream once.
#[derive(Clone)]
pub struct ApiHandler {
    pending: Arc<DashMap<CacheKey, Pending>>,
    cache: CacheRouter
}

impl ApiHandler {
    pub fn new() -> Self {
        Self {
            pending: Arc::new(DashMap::with_capacity(20)), // Max capacity of 20 requests. we really shouldnt be getting more than 2-3 MAX. Often only 1.
            cache: CacheRouter::new()
        }
    }
    
    pub async fn get(&self, key: CacheKey, processer: fn(Bytes) -> Result<Bytes, ProcessError>) -> Result<Bytes, ProcessError> {
        if let Some(data) = self.cache.get(key).await {
            return Ok(data)
        }
        
        match self.pending.entry(key) {
            Entry::Occupied(entry) => {
                log(LogMessage::MessageAndUser { id: key.uuid(), message: "Awaiting Hypixel Hit" });
                entry.get().clone()
            }
            Entry::Vacant(entry) => {
                let internal = self.clone();

                let request_future = async move {
                    let now = Instant::now();
                    let req = request(key.hypixel_url()).await.and_then(processer);
                    let hypixel_time = now.elapsed();
                    
                    if let Ok(data) = req.as_ref() {
                        internal.cache.put(key, data).await;
                    } 
                    
                    internal.pending.remove(&key);
                    log(LogMessage::DoubleElapsed { id: key.uuid(), first_elapsed: hypixel_time, second_elapsed: now.elapsed(), message: "Hypixel Hit"});
                    req
                }.boxed().shared();

                entry.insert(request_future.clone());
                request_future
            }
        }.await
    }
}

async fn request(url: String) -> Result<Bytes, ProcessError> {
    let res = CLIENT.get(&url).send().await?;
    res.error_for_status()?.bytes().await.map_err(ProcessError::from)
}