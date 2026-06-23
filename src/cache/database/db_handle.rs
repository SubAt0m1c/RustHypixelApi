use std::{io, time::Duration};

use actix_web::{web::Bytes};
use tokio::sync::{mpsc::{unbounded_channel, UnboundedSender}, oneshot};

use crate::cache::{UuidKey, compression::{compress_data, extract_data}, database::{db_message::DbMessage, spawn_db_worker}};

pub struct DbHandle {
    compress_tx: UnboundedSender<(UuidKey, Duration, Bytes)>,
    db_tx: UnboundedSender<DbMessage>,
}

impl DbHandle {
    pub fn new() -> Self {
        let (db_tx, db_rx) = unbounded_channel::<DbMessage>();
        let (compress_tx, mut compress_rx) = unbounded_channel::<(UuidKey, Duration, Bytes)>();
        
        spawn_db_worker(db_rx);
        let worker_tb_tx = db_tx.clone();
        
        // Worker thread for compresion. This moves compression (cpu heavy work) off of the io-heavy database worker task and user tasks.
        // We may want to do this for json parsing at some point as well but we will see.
        // Should be parallelized into a pool if this ends up getting backed up, but at our scale this is fine until we get more requests
        // than we can compress. (existing issue for previous models anyways though)
        // 
        // Also could just be a tokio spawn_blocking task (and probably should be if we try to parallelize) but for now 
        // using a std thread indicates the nature of the blocking permenant lifecycle rather than the idea of tokio's
        // blocking tasks being short-lived. Doesnt really matter though.
        std::thread::spawn(move || {
            while let Some((id, ttl, data)) = compress_rx.blocking_recv() {
                let compressed = compress_data(&data);
                worker_tb_tx.send(DbMessage::Write { id, ttl, data: compressed }).expect("Db channel shouldnt close");
            }
        });
        
        Self {
            compress_tx,
            db_tx: db_tx,
        }
    }

    pub fn write(&self, key: UuidKey, ttl: Duration, data: Bytes) {
        self.compress_tx.send((key, ttl, data)).expect("DB channel shouldnt close");
    }

    pub async fn read(&self, id: UuidKey) -> Result<Option<Bytes>, io::Error> {
        let (tx, rx) = oneshot::channel();
        self.db_tx.send(DbMessage::Read { id, res: tx }).expect("DB channel shouldnt close");
        let Some(data) = rx.await.map_err(io::Error::other)? else {
            return Ok(None)
        };

        Ok(Some(Bytes::from(extract_data(&data).map_err(io::Error::other)?)))
    }
}