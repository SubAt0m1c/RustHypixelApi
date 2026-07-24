use std::sync::atomic::Ordering;

use actix_web::{HttpResponse, Responder, get, web::Data};
use portable_atomic::AtomicU128;
use reqwest::header::HeaderMap;
use simd_json::json;

pub struct RateLimit {
    inner: AtomicU128
}

impl RateLimit {
    pub fn new() -> Self {
        Self {
            inner: AtomicU128::new(0),
        }
    }

    pub fn store(&self, remaining: u64, reset: u64, order: Ordering) {
        let value = u128::from(remaining) << 64 | u128::from(reset);
        self.inner.store(value, order);
    }

    pub fn load(&self, order: Ordering) -> (u64, u64) {
        let value = self.inner.load(order);
        let remaining = (value >> 64) as u64;
        #[allow(clippy::cast_possible_truncation)]
        let reset = value as u64;
        (remaining, reset)
    }
}

pub fn stats_from_headers(headers: &HeaderMap) -> Option<(u64, u64)> {
    let remaining = headers.get("RateLimit-Remaining")?.to_str().ok()?.parse().ok()?;
    let reset = headers.get("RateLimit-Reset")?.to_str().ok()?.parse().ok()?;
    Some((remaining, reset))
}

#[get("/stats")]
async fn statistics(
    rate_limit: Data<RateLimit>,
) -> actix_web::Result<impl Responder> {
    let (remaining, reset) = rate_limit.load(Ordering::Relaxed);
    let json = json!({
        "RateLimit-Remaining": remaining,
        "RateLimit-Reset": reset
    });
    Ok(HttpResponse::Ok().json(json))
}
