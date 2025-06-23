use crate::cache_enum::CacheEnum;
use crate::fetch_and_cache::fetch_and_cache;
use crate::format::format_numbers;
use actix_web::{get, web, Responder};
use reqwest::Client;
use std::sync::Arc;
use std::time::Duration;

#[get("/get/{uuid}")]
async fn profile(
    path: web::Path<String>,
    client: web::Data<Arc<Client>>,
    cache: web::Data<CacheEnum>
) -> actix_web::Result<impl Responder> {
    let uuid = path.into_inner().replace("-", "");
    let url = format!("https://api.hypixel.net/v2/skyblock/profiles?uuid={}", uuid);
    let cache_id = format!("{} by P", uuid);

    fetch_and_cache(url, cache_id, client, cache, Duration::from_secs(600), format_numbers).await
}