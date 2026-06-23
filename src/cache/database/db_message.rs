use std::time::Duration;

use actix_web::web::Bytes;
use tokio::sync::oneshot::Sender;

use crate::cache::{UuidKey, database::CompressedBytes};

pub enum DbMessage {
    Write {
        id: UuidKey,
        ttl: Duration,
        data: CompressedBytes,
    },
    Read {
        id: UuidKey,
        res: Sender<Option<Bytes>>
    }
}