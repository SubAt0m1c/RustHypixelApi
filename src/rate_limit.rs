use chrono::Utc;
use dashmap::DashMap;
use rocket::http::Status;
use rocket::outcome::Outcome;
use rocket::Request;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::RwLock;

pub type RateLimitMap = Arc<DashMap<String, RateLimits>>;

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

pub struct RateTracker {
    requests: AtomicU64,
    restart_time: RwLock<Instant>,
}

impl RateTracker {
    pub fn new() -> Self {
        RateTracker {
            requests: AtomicU64::new(0),
            restart_time: RwLock::new(Instant::now()),
        }
    }

    pub async fn requests(&self) -> u64 {
        self.requests.load(Ordering::Relaxed)
    }

    pub async fn elapsed(&self) -> Duration {
        self.restart_time.read().await.elapsed()
    }

    pub async fn inc(&self, start_time: &Instant) {
        let read_time = self.restart_time.read().await;
        let elapsed = start_time.duration_since(*read_time).as_secs();
        if elapsed > 300 {
            self.reset(start_time).await;
        }

        self.requests.fetch_add(1, Ordering::Relaxed);
    }

    async fn reset(&self, start_time: &Instant) {
        self.requests.store(0, Ordering::Relaxed);
        let mut time = self.restart_time.write().await;
        *time = *start_time;
    }
}

pub struct RateLimits {
    profile: RateLimit,
    secrets: RateLimit,
}

impl RateLimits {
    fn new() -> Self {
        RateLimits {
            profile: RateLimit::new(),
            secrets: RateLimit::new(),
        }
    }
}

struct RateLimit {
    tokens: u64,
    last_refill: Instant,
}

impl RateLimit {
    fn new() -> Self {
        RateLimit {
            tokens: MAXIMUM_TOKENS,
            last_refill: Instant::now(),
        }
    }
}

pub struct RateLimiter;

#[rocket::async_trait]
impl<'r> rocket::request::FromRequest<'r> for RateLimiter {
    type Error = ();

    async fn from_request(req: &'r Request<'_>) -> rocket::request::Outcome<Self, Self::Error> {
        let now = Instant::now();

        println!(
            "----\nrequest timestamp: {}",
            Utc::now().with_timezone(&chrono::Local).to_rfc2822()
        );

        let limiter = req.rocket().state::<RateLimitMap>().unwrap();
        let limiter_length = limiter.len();

        let client_ip = req.client_ip().map(|ip| ip.to_string()).unwrap_or_default();

        let mut entry = limiter
            .entry(client_ip.clone())
            .or_insert(RateLimits::new());
        let RateLimit {
            ref mut tokens,
            ref mut last_refill,
        } = if req.uri().path().starts_with("/secrets/") {
            &mut entry.secrets
        } else {
            &mut entry.profile
        };

        let time_since_last_refill = now.duration_since(*last_refill);
        let tokens_to_add = ((time_since_last_refill.as_secs_f64() / TIME_WINDOW.as_secs_f64())
            * MAXIMUM_TOKENS as f64) as u64;

        if tokens_to_add > 0 {
            *tokens = (*tokens + tokens_to_add).min(MAXIMUM_TOKENS);
            *last_refill += Duration::from_secs_f64(
                tokens_to_add as f64 * TIME_WINDOW.as_secs_f64() / MAXIMUM_TOKENS as f64,
            );
        }

        let outcome = if *tokens > 0 {
            *tokens -= 1;
            Outcome::Success(RateLimiter)
        } else {
            Outcome::Error((Status::TooManyRequests, ()))
        };

        println!(
            "Client: {},\nTokens: profile: {} filled {}s ago | secrets: {} filled {}s ago,\nRateLimitSize: {}",
            client_ip.clone(), // used for debugging purposes.
            entry.profile.tokens,
            entry.profile.last_refill.elapsed().as_secs_f64(),
            entry.secrets.tokens,
            entry.secrets.last_refill.elapsed().as_secs_f64(),
            limiter_length
        );

        drop(entry); // entry has to be dropped here otherwise clean_cache() would hang.

        if (limiter_length) % IP_THRESHOLD == 0 || limiter_length >= MAX_CACHE_SIZE {
            clean_cache(&limiter, &now);
        }

        outcome
    }
}

fn clean_cache(limiter: &RateLimitMap, now: &Instant) {
    let mut retained_count = 0;
    let mut oldest_key: Option<String> = None;
    let mut oldest_time = *now;

    limiter.retain(|key, rate_limit_data| {
        let mut keep_entry = false;

        for RateLimit {
            tokens,
            last_refill,
        } in [&mut rate_limit_data.profile, &mut rate_limit_data.secrets]
        {
            let time_since_last_refill = now.duration_since(*last_refill);

            let tokens_to_add = (time_since_last_refill.as_secs_f64() / TIME_WINDOW.as_secs_f64())
                * MAXIMUM_TOKENS as f64;
            let total_tokens = (*tokens as f64 + tokens_to_add).min(MAXIMUM_TOKENS as f64) as u64;

            if total_tokens < MAXIMUM_TOKENS {
                *tokens = total_tokens;
                *last_refill = *now;
                keep_entry = true;

                if *last_refill < oldest_time {
                    oldest_time = *last_refill;
                    oldest_key = Some(key.clone());
                }
            }
        }

        if keep_entry {
            retained_count += 1;
        }

        keep_entry
    });

    if retained_count > MAX_CACHE_SIZE {
        if let Some(oldest) = oldest_key {
            limiter.remove(&oldest);
        }
    }
}
