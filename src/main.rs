mod cache;
mod key_extractor;
mod routes;
mod timer;
mod utils;
mod api_handler;
mod logging;
mod error;

use crate::api_handler::ApiHandler;

use crate::key_extractor::RealKeyExtractor;
use crate::routes::profile::profile;
use crate::routes::secrets::secrets;
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
    logging::init();
    
    let apikey = std::env::var("API_KEY").expect("no api key env variable found");
    let ip_addr: String = std::env::var("IP_ADDR").unwrap_or("127.0.0.1".to_string());
    println!("Listening on {}:8000!", ip_addr);
    
    let mut headers = HeaderMap::new();
    headers.insert("API-Key", apikey.parse().unwrap());

    let rate_limit = GovernorConfigBuilder::default()
        .key_extractor(RealKeyExtractor)
        .seconds_per_request(3)
        .burst_size(10)
        .finish()
        .unwrap();

    let cache = Data::new(ApiHandler::new());
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
            .wrap(from_fn(timer::timer))
            .service(secrets)
            .service(profile)
    })
    .bind((ip_addr, 8000))?
    .run()
    .await
}
