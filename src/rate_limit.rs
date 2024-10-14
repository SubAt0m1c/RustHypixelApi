use rocket::Request;
use std::sync::Arc;
use std::time::{Instant, Duration};
use dashmap::DashMap;
use rocket::http::Status;
use rocket::outcome::Outcome;

type RateLimit = Arc<DashMap<String, Vec<Instant>>>;

///Request limit per [TIME_WINDOW]
const REQUEST_LIMIT: u64 = 5;
///Window for requests to expire
const TIME_WINDOW: Duration = Duration::from_secs(30);
///Entry multiple which clears the map. Ie: if the value is 25, the 25th, 50th, 75th, etc. value will clean the cache of expired entries.
/// set to 0 to clean the map every time a value is accessed
const IP_THRESHOLD: usize = 25;
///Maximum size of the ip cache. This method technically allows a brute force to spam from different ips to effectively disable the rate limit,
/// however I don't know a better solution that wouldn't effectively keep increasing memory usage.
const MAX_CACHE_SIZE: usize = 100;

pub struct RateLimiter;

#[rocket::async_trait]
impl<'r> rocket::request::FromRequest<'r> for RateLimiter {
    type Error = ();

    async fn from_request(req: &'r Request<'_>) -> rocket::request::Outcome<Self, Self::Error> {
        let now = Instant::now();

        let limiter = req.rocket().state::<RateLimit>().unwrap();

        let limiter_length = limiter.len();

        let client_ip = req.client_ip().map(|ip| ip.to_string()).unwrap_or_default();

        {
            let mut entry = limiter.entry(client_ip.clone()).or_insert(vec![]);

            entry.retain(|&time| now.duration_since(time) <= TIME_WINDOW);

            if entry.len() >= REQUEST_LIMIT as usize {
                return Outcome::Error((Status::TooManyRequests, ()));
            }

            entry.push(now);
        }

        if (limiter_length) % IP_THRESHOLD == 0 || limiter_length >= MAX_CACHE_SIZE {
            clean_cache(&limiter, now);
        }

        Outcome::Success(RateLimiter)
    }
}

fn clean_cache(limiter: &RateLimit, now: Instant) {
    let mut oldest_key: Option<String> = None;
    let mut oldest_time = now;
    let mut retained_count = 0;

    limiter.retain(|key, time_stamps| {
        time_stamps.retain(|&time| now.duration_since(time) <= TIME_WINDOW);

        if let Some(&last) = time_stamps.last() {
            if last < oldest_time {
                oldest_time = last;
                oldest_key = Some(key.clone());
            }
        }

        if !time_stamps.is_empty() {
            retained_count += 1;
            return true
        }
        false
    });

    if retained_count > MAX_CACHE_SIZE {
        if let Some(key) = oldest_key {
            limiter.remove(&key);
        }
    }
}