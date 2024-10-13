use rocket::Request;
use std::sync::Arc;
use dashmap::DashMap;
use std::time::{Instant, Duration};
use rocket::http::Status;
use rocket::outcome::Outcome;

type RateLimit = Arc<DashMap<String, (u64, Instant)>>;

///Request limit per [TIME_WINDOW]
const REQUEST_LIMIT: u64 = 5;
///Window for requests to expire
const TIME_WINDOW: Duration = Duration::from_secs(30);

pub struct RateLimiter;

#[rocket::async_trait]
impl<'r> rocket::request::FromRequest<'r> for RateLimiter {
    type Error = ();

    async fn from_request(req: &'r Request<'_>) -> rocket::request::Outcome<Self, Self::Error> {
        let limiter = req.rocket().state::<RateLimit>().unwrap();
        let client_ip = req.client_ip().map(|ip| ip.to_string()).unwrap_or_default();
        let mut entry = limiter.entry(client_ip.clone()).or_insert((0, Instant::now()));

        let (ref mut count, ref mut last_request) = *entry;

        if last_request.elapsed() > TIME_WINDOW {
            *count = 1;
            *last_request = Instant::now();
        } else if *count < REQUEST_LIMIT {
            *count += 1;
        } else {
            return Outcome::Error((Status::TooManyRequests, ()));
        }

        Outcome::Success(RateLimiter)
    }
}
