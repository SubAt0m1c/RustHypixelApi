use actix_web::body::MessageBody;
use actix_web::dev::{ServiceRequest, ServiceResponse};
use actix_web::middleware::Next;
use std::time::Instant;

pub async fn timer(
    req: ServiceRequest,
    next: Next<impl MessageBody>,
) -> Result<ServiceResponse<impl MessageBody>, actix_web::Error> {
    let now = Instant::now();
    let res = next.call(req).await;
    let time_taken = now.elapsed();
    println!("Time taken: {:?}", time_taken);
    res
}
