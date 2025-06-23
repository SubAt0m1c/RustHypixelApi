use crate::cache_enum::CacheEnum;
use actix_web::{get, web, Responder};
use reqwest::Client;
use std::sync::Arc;
use std::time::Duration;
use crate::fetch_and_cache::fetch_and_cache;
use crate::format::format_secrets;

#[get("/secrets/{uuid}")]
async fn secrets(
    path: web::Path<String>,
    client: web::Data<Arc<Client>>,
    cache: web::Data<CacheEnum>
) -> actix_web::Result<impl Responder> {
    let uuid = path.into_inner().replace("-", "");
    let url = format!("https://api.hypixel.net/v2/player?uuid={}", uuid);
    let cache_id = format!("{} by P", uuid);
    
    fetch_and_cache(url, cache_id, client, cache, Duration::from_secs(60), format_secrets).await
}