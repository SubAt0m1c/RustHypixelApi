mod cache;
mod format;
mod rate_limit;
mod routes;

use crate::rate_limit::RateLimitMap;
use crate::routes::{handle_players, handle_secrets};
use cache::Cache;
use dashmap::DashMap;
use rocket::routes;
use std::sync::{Arc, Mutex};

type SharedCache = Arc<Mutex<Cache>>;

//todo: Secrets endpoint with lower caching and

#[tokio::main]
async fn main() {
    let cache = Cache::create();
    let rate_limit: RateLimitMap = Arc::new(DashMap::new());
    let secret_rate_limit: RateLimitMap = Arc::new(DashMap::new());
    let args: Vec<String> = std::env::args().collect();

    if args.len() < 2 {
        eprintln!("Usage: cargo run <API_KEY>");
        return;
    }

    let api_key = &args[1];

    rocket::build()
        .manage(cache)
        .manage(api_key.clone())
        .manage(rate_limit)
        .mount("/get/", routes![handle_players])
        .mount("/secrets/", routes![handle_secrets])
        .launch()
        .await
        .unwrap();
}
