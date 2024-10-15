use dashmap::DashMap;
use rocket::http::Status;
use rocket::outcome::Outcome;
use rocket::Request;
use std::sync::Arc;
use std::time::{Duration, Instant};

pub type RateLimitMap = Arc<DashMap<String, (u64, Instant)>>;

///maximum tokens per [TIME_WINDOW]. One token will regenerate every [TIME_WINDOW] / [MAXIMUM_TOKENS] seconds, ex: 30s / 5 tokens = 1 token every 6 seconds, with a max of 5.
const MAXIMUM_TOKENS: u64 = 5;
///Window for requests to expire
const TIME_WINDOW: Duration = Duration::from_secs(30);
///Entry multiple which clears the map. Ie: if the value is 25, the 25th, 50th, 75th, etc. value will clean the cache of expired entries.
///
///Set to 0 to clean the map every time a value is accessed
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

        let limiter = req.rocket().state::<RateLimitMap>().unwrap();
        let limiter_length = limiter.len();

        let client_ip = req.client_ip().map(|ip| ip.to_string()).unwrap_or_default();

        let mut entry = limiter
            .entry(client_ip.clone())
            .or_insert((MAXIMUM_TOKENS, now));
        let (ref mut tokens, ref mut last_refill) = *entry.value_mut();

        let time_since_last_refill = now.duration_since(*last_refill);
        let tokens_to_add = (time_since_last_refill.as_secs_f64() / TIME_WINDOW.as_secs_f64())
            * MAXIMUM_TOKENS as f64;
        *tokens = (*tokens + tokens_to_add as u64).min(MAXIMUM_TOKENS);

        let outcome = if *tokens > 0 {
            *last_refill = now;
            *tokens -= 1;

            println!("Tokens remaining: {}", tokens);
            Outcome::Success(RateLimiter)
        } else {
            println!("last token refil: {}s ago", last_refill.elapsed().as_secs());
            Outcome::Error((Status::TooManyRequests, ()))
        };

        drop(entry);

        if (limiter_length) % IP_THRESHOLD == 0 || limiter_length >= MAX_CACHE_SIZE {
            clean_cache(&limiter, &now);
        }

        outcome
    }
}

fn clean_cache(limiter: &RateLimitMap, &now: &Instant) {
    let mut retained_count = 0;
    let mut oldest_key: Option<String> = None;
    let mut oldest_time = now.clone();

    limiter.retain(|key, &mut (tokens, last_refill)| {
        let time_since_last_refill = now.duration_since(last_refill);

        let tokens_to_add = (time_since_last_refill.as_secs_f64() / TIME_WINDOW.as_secs_f64())
            * MAXIMUM_TOKENS as f64;
        let total_tokens = (tokens as f64 + tokens_to_add).min(MAXIMUM_TOKENS as f64) as u64;

        if total_tokens >= MAXIMUM_TOKENS {
            false
        } else {
            retained_count += 1;
            if last_refill < oldest_time {
                oldest_time = last_refill;
                oldest_key = Some(key.clone());
            }
            true
        }
    });

    if retained_count > MAX_CACHE_SIZE {
        if let Some(oldest) = oldest_key {
            limiter.remove(&oldest);
        }
    }
}
