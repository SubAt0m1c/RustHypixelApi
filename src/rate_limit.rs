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

pub struct RateLimiter;

#[rocket::async_trait]
impl<'r> rocket::request::FromRequest<'r> for RateLimiter {
    type Error = ();

    async fn from_request(req: &'r Request<'_>) -> rocket::request::Outcome<Self, Self::Error> {
        let now = Instant::now();

        let limiter = req.rocket().state::<RateLimit>().unwrap();

        let limiter_length = limiter.len();

        let client_ip = req.client_ip().map(|ip| ip.to_string()).unwrap_or_default();
        let mut entry = limiter.entry(client_ip.clone()).or_insert(vec![]);

        entry.retain(|&time| now.duration_since(time) <= TIME_WINDOW);

        if entry.len() > REQUEST_LIMIT as usize {
            return Outcome::Error((Status::TooManyRequests, ()));
        }

        if limiter_length+1 % IP_THRESHOLD == 0 {
            limiter.retain(|_, time_stamps| {
                if let Some(&last) = time_stamps.last() {
                    now.duration_since(last) <= TIME_WINDOW
                } else { false }
            });
        }

        entry.push(now);
        Outcome::Success(RateLimiter)
    }
}
