use actix_web::body::MessageBody;
use actix_web::dev::{ServiceRequest, ServiceResponse};
use actix_web::middleware::Next;
use std::time::Instant;

use crate::logging::{LogMessage, log};
pub async fn timer(
    req: ServiceRequest,
    next: Next<impl MessageBody>,
) -> Result<ServiceResponse<impl MessageBody>, actix_web::Error> {
    let now = Instant::now();
    let res = next.call(req).await;
    let elapsed = now.elapsed();
    log(LogMessage::TimeElapsed { elapsed, name: "the last request" });
    res
}
