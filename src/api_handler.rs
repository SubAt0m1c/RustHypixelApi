use std::sync::Arc;

use actix_web::web::{Bytes, Data};
use dashmap::{DashMap, Entry};
use futures::{FutureExt, future::{BoxFuture, Shared}};
use reqwest::Client;

use crate::{cache::{cache_key::CacheKey, cache_router::CacheRouter}, error::ProcessError, logging::{LogMessage, log}};
type Pending = Shared<BoxFuture<'static, Result<Bytes, ProcessError>>>;

#[derive(Clone)]
pub struct ApiHandler {
    pending: Arc<DashMap<CacheKey, Pending>>,
    cache: CacheRouter
}

impl ApiHandler {
    pub fn new() -> Self {
        Self {
            pending: Arc::new(DashMap::with_capacity(20)),
            cache: CacheRouter::new()
        }
    }
    
    pub async fn get(&self, key: CacheKey, client: Data<Client>, processer: fn(Bytes) -> Result<Bytes, ProcessError>) -> Result<Bytes, ProcessError> {
        if let Some(data) = self.cache.get(key).await {
            log(LogMessage::MessageAndUser { id: key.uuid(), message: "Pulled user from cache" });
            return Ok(data)
        }
        
        match self.pending.entry(key) {
            Entry::Occupied(entry) => {
                log(LogMessage::AwaitingSameRequest { id: key.uuid() });
                entry.get().clone()
            }
            Entry::Vacant(entry) => {
                let url = key.hypixel_url();
                let internal = self.clone();

                let request_future = async move {
                    let req = request(url, &client.clone()).await.and_then(processer);
    
                    log(LogMessage::MessageAndUser { id: key.uuid(), message: "Placed user into cache" });
                    
                    if let Ok(data) = req.as_ref() {
                        internal.cache.put(key, data).await;
                    } 
                    
                    internal.pending.remove(&key);
                    req
                }.boxed().shared();

                entry.insert(request_future.clone());
                request_future
            }
        }.await
    }
}

async fn request(url: String, client: &Client) -> Result<Bytes, ProcessError> {
    let res = client.get(&url).send().await?;
    res.error_for_status()?.bytes().await.map_err(ProcessError::from)
}