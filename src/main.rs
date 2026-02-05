mod cache;
mod key_extractor;
mod routes;
mod timer;
mod utils;

use crate::cache::moka_cache::MokaCache;
use crate::key_extractor::RealKeyExtractor;
use crate::routes::dungeon::dungeon_info;
use crate::routes::{profile::profile, secrets::secrets};
use actix_governor::{Governor, GovernorConfigBuilder};
use actix_web::middleware::from_fn;
use actix_web::web::Data;
use actix_web::{App, HttpServer};
use mimalloc::MiMalloc;
use reqwest::header::HeaderMap;
use reqwest::Client;

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;


#[actix_web::main]
async fn main() -> std::io::Result<()> {
    let apikey = std::env::var("API_KEY").expect("no api key env variable found");

    let mut headers = HeaderMap::new();
    headers.insert("API-Key", apikey.parse().unwrap());

    let rate_limit = GovernorConfigBuilder::default()
        .key_extractor(RealKeyExtractor)
        .seconds_per_request(3)
        .burst_size(10)
        .finish()
        .unwrap();

    let cache = Data::new(MokaCache::new());
    let client = Data::new(
        Client::builder()
            .default_headers(headers.clone())
            .build()
            .unwrap(),
    );

    HttpServer::new(move || {
        App::new()
            .app_data(cache.clone())
            .app_data(client.clone())
            .wrap(Governor::new(&rate_limit))
            .wrap(from_fn(timer::timer)) //println io is expensive...
            .service(secrets)
            .service(profile)
            .service(dungeon_info)
    })
    .bind(("127.0.0.1", 8000))?
    .run()
    .await
}
