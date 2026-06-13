use std::time::Instant;

use actix_web::{cookie::time::UtcDateTime, web::Bytes};
use uuid::Uuid;

pub struct BatchState {
    pub pending_writes: Vec<(Uuid, Bytes, UtcDateTime)>,
    pub start_time: Option<Instant>,
    pub last_write_time: Option<Instant>,
}

impl BatchState {
    pub fn new(size: usize) -> Self {
        BatchState {
            pending_writes: Vec::with_capacity(size), //todo: max batch size parameter
            start_time: None,
            last_write_time: None,
        }
    }
}