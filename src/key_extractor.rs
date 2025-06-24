use actix_governor::{KeyExtractor, SimpleKeyExtractionError};
use actix_web::dev::ServiceRequest;
use std::net::IpAddr;

#[derive(Clone)]
pub struct RealKeyExtractor;

impl KeyExtractor for RealKeyExtractor {
    type Key = IpAddr;
    type KeyExtractionError = SimpleKeyExtractionError<&'static str>;

    fn extract(&self, req: &ServiceRequest) -> Result<Self::Key, Self::KeyExtractionError> {
        req.connection_info()
            .realip_remote_addr()
            .and_then(|ip| ip.parse::<IpAddr>().ok())
            .ok_or(SimpleKeyExtractionError::new("No remote address"))
    }
}
