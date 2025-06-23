mod secrets;
mod profile;
mod fetch_and_cache;
mod moka_cache;
mod format;
mod timer;
mod lru_cache;
mod cache_enum;

use crate::moka_cache::MokaCache;
use actix_governor::{Governor, GovernorConfigBuilder};
use actix_web::middleware::from_fn;
use actix_web::web::Data;
use actix_web::{App, HttpServer, Responder};
use reqwest::header::HeaderMap;
use reqwest::Client;
use std::sync::Arc;

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    let apikey = std::env::var("API_KEY").unwrap();
    
    let mut headers = HeaderMap::new();
    headers.insert("API-Key", apikey.parse().unwrap());
    
    let rate_limit = GovernorConfigBuilder::default()
        .seconds_per_request(3)
        .burst_size(10)
        .finish()
        .unwrap();
    
    // these need to be arced since the app::new() is run every time a new task or maybe thread is used. (Moka cache is internally arced)
    let cache = cache_enum::CacheEnum::MOKA(MokaCache::new());
    let client = Arc::new(Client::builder().default_headers(headers.clone()).build().unwrap());
    
    HttpServer::new(move || {
        App::new()
            .app_data(Data::new(cache.clone()))
            .app_data(Data::new(client.clone()))
            .wrap(Governor::new(&rate_limit))
            .wrap(from_fn(timer::timer))
            .service(secrets::secrets)
            .service(profile::profile)
    })
        .bind(("127.0.0.1", 8000))?
        .run()
        .await
}