mod cache;
mod format;
mod rate_limit;
mod rate_tracker;
mod routes;

use crate::rate_limit::RateLimitMap;
use crate::routes::{handle_players, handle_secrets};
use cache::Cache;
use dashmap::DashMap;
use reqwest::header::{HeaderMap, HeaderValue, CONTENT_TYPE};
use rocket::routes;
use std::sync::{Arc, Mutex};

type SharedCache = Arc<Mutex<Cache>>;

#[tokio::main]
async fn main() {
    let cache = Cache::create();
    let rate_limit: RateLimitMap = Arc::new(DashMap::new());
    //let rate_tracker: RateTracker = RateTracker::new();
    let args: Vec<String> = std::env::args().collect();

    if args.len() < 2 {
        eprintln!("Usage: cargo run <API_KEY>");
        return;
    }

    let mut headers = HeaderMap::new();
    headers.insert(
        CONTENT_TYPE,
        HeaderValue::from_str("application/json").unwrap(),
    );
    headers.insert("API-Key", HeaderValue::from_str(&args[1]).unwrap());

    let client = reqwest::Client::builder()
        .default_headers(headers)
        .build()
        .unwrap();

    rocket::build()
        .manage(cache)
        .manage(client)
        .manage(rate_limit)
        //.manage(rate_tracker)
        .mount("/get/", routes![handle_players])
        .mount("/secrets/", routes![handle_secrets])
        .launch()
        .await
        .unwrap();
}
