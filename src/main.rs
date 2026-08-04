use std::{env, fmt::{Debug, Display}, str::FromStr, sync::OnceLock};

use actix_governor::{Governor, GovernorConfigBuilder};
use actix_web::{App, HttpServer, middleware::from_fn, web::Data};
use mimalloc::MiMalloc;

use crate::{cache::cache_router::CacheRouter, key_extractor::RealKeyExtractor, routes::{profile::profile, secrets::secrets, stats::{RateLimit, statistics}}};

mod cache;
mod key_extractor;
mod routes;
mod timer;
mod request_utils;
mod logging;
mod error;

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

pub static API_KEY: OnceLock<String> = OnceLock::new();

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    logging::init();

    let api_key = std::env::var("API_KEY").expect("no api key env variable found");
    API_KEY.set(api_key).expect("API_KEY should be available to set!");
    let ip_addr: String = std::env::var("IP_ADDR").unwrap_or("127.0.0.1".to_string());
    println!("Listening on {ip_addr}:8000!");

    let rate_limit = GovernorConfigBuilder::default()
        .key_extractor(RealKeyExtractor)
        .seconds_per_request(env_var("RATELIMIT_REFRESH", 3))
        .burst_size(env_var("RATELIMIT_BURST", 10))
        .finish()
        .unwrap();

    let stats = Data::new(RateLimit::new());
    let cache = Data::new(CacheRouter::load().await.unwrap());

    HttpServer::new(move || {
        App::new()
            .app_data(stats.clone())
            .app_data(cache.clone())
            .wrap(Governor::new(&rate_limit))
            .wrap(from_fn(timer::timer))
            .service(secrets)
            .service(profile)
            .service(statistics)
    })
    .bind((ip_addr, 8000))?
    .run()
    .await
}

/// # Panics
/// panics if the environment variable is not parsable as `T`.
pub fn env_var<T>(key: &'static str, default: T) -> T
where
    T: FromStr + Display,
    T::Err: Debug
{
    match env::var(key) {
        Ok(str) => str.parse::<T>().unwrap_or_else(|e| panic!("{} should be a {}!: {e:?}", key, std::any::type_name::<T>())),
        Err(e) => {
            eprintln!("{e}: {key}, using {default} default.");
            default
        }
    }
}