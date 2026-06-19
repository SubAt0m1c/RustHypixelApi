use actix_web::web::Bytes;
use tokio::sync::oneshot::Sender;
use uuid::Uuid;

use crate::cache::database::CompressedBytes;

pub enum DbMessage {
    Write {
        id: Uuid,
        data: CompressedBytes,
    },
    Read {
        id: Uuid,
        res: Sender<Option<Bytes>>
    }
}