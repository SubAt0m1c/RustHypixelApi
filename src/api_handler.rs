use std::{io::Error, sync::Arc};

use actix_web::web::{Bytes, Data};
use futures::{FutureExt, future::{BoxFuture, Shared}};
use moka::future::Cache;
use reqwest::Client;

use crate::{cache::{cache_router::CacheRouter, moka_cache::{MokaCache, MokaKey}}, logging::{LogMessage, log}, utils::fetch};

const MAX_PENDING: u64 = 20;

type Pending = Shared<BoxFuture<'static, Option<Bytes>>>;

#[derive(Clone)]
pub struct ApiHandler {
    pending: Cache<MokaKey, Pending>,
    cache: CacheRouter
}

impl ApiHandler {
    pub fn new() -> Self {
        let pending = Cache::new(MAX_PENDING);
        Self {
            pending,
            cache: CacheRouter::new(MokaCache::new())
        }
    }

    pub async fn get(&self, key: MokaKey, client: Data<Client>, processer: fn(Bytes) -> Option<Bytes>) -> Option<Bytes> {
        if let Some(data) = self.cache.get(key).await {
            log(LogMessage::MessageAndUser { id: key.uuid(), message: "Pulled user from cache" });
            return Some(data)
        }

        if let Some(fut) = self.pending.get(&key).await {
            log(LogMessage::AwaitingSameRequest { id: key.uuid() });
            fut.await
        } else {
            let url = match key {
                MokaKey::Profile(id) => format!("https://api.hypixel.net/v2/skyblock/profiles?uuid={}", id),
                MokaKey::Secrets(id) => format!("https://api.hypixel.net/v2/player?uuid={}", id),
            };
            let internal = self.clone();

            let fut = async move {
                let req = request(url, &client.clone()).await.and_then(processer);

                log(LogMessage::MessageAndUser { id: key.uuid(), message: "Placed user into cache" });
                
                if let Some(data) = req.as_ref() {
                    internal.cache.put(key, data).await;
                } 
                
                internal.pending.invalidate(&key).await;
                req
            }.boxed().shared();
            self.pending.insert(key, fut.clone()).await;

            fut.await
        }
    }
}

async fn request(url: String, client: &Client) -> Option<Bytes> {
    let res = client.get(&url).send().await.ok()?;
    res.error_for_status().ok()?.bytes().await.ok()
}