use actix_web::web::Bytes;
use tokio::sync::oneshot::Sender;
use uuid::Uuid;

pub enum DbMessage {
    Write {
        id: Uuid,
        data: Bytes,
    },
    Read {
        id: Uuid,
        res: Sender<Option<Bytes>>
    }
}