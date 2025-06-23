use crate::cache::cache_enum::CacheEnum;
use crate::utils::{fetch, json_response};
use actix_web::error::ErrorInternalServerError;
use actix_web::{get, web, HttpResponse, Responder};
use reqwest::Client;
use std::sync::Arc;
use std::time::Duration;

#[get("/get/{uuid}")]
async fn profile(
    path: web::Path<String>,
    client: web::Data<Arc<Client>>,
    cache: web::Data<CacheEnum>,
) -> actix_web::Result<impl Responder> {
    let uuid = path.into_inner().replace("-", "");
    let url = format!("https://api.hypixel.net/v2/skyblock/profiles?uuid={}", uuid);
    let cache_key = format!("{} by P", uuid);

    if let Some(cached) = cache.get(&cache_key, Duration::from_secs(600)).await {
        return Ok(json_response(cached));
    }

    let bytes = fetch(url, client).await.map_err(ErrorInternalServerError)?;
    //let str = String::from_utf8_lossy(&bytes).into_owned();

    cache
        .insert(cache_key, bytes.clone(), Duration::from_secs(600))
        .await;

    Ok(json_response(bytes))
}
