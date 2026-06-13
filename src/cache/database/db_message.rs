use actix_web::web::Bytes;
use tokio::sync::oneshot::Sender;
use uuid::Uuid;

use crate::cache::database::db_entry::DbEntry;

pub enum DbMessage {
    Write {
        id: Uuid,
        data: DbEntry,
    },
    Read {
        id: Uuid,
        res: Sender<Option<Bytes>>
    }
}