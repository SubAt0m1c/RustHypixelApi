use std::{path::Path, sync::{LazyLock, Once, OnceLock}};

use actix_web::{cookie::time::UtcDateTime, web::Bytes as Bytes};
use heed::{Database, Env, EnvOpenOptions, Error, byteorder::{self, ByteOrder}, types::{ Bytes as ByteSlice, I64, SerdeBincode, U128}};
use tokio::sync::{mpsc::{UnboundedSender, unbounded_channel}, oneshot::{Receiver, Sender, channel}};
use uuid::Uuid;

static ENVIRONMENT: LazyLock<Env> = LazyLock::new(|| {
    unsafe { EnvOpenOptions::new().open(".db") }.unwrap()
});

pub enum DbMessage {
    Write {
        id: Uuid,
        data: Bytes,
        added: UtcDateTime,
    },
    Read {
        id: Uuid,
        res: Sender<Bytes>
    }
}

pub struct DbHandle {
    tx: UnboundedSender<DbMessage>
}
impl DbHandle {
    pub fn new() -> Self {
        let (tx, mut rx) = unbounded_channel::<DbMessage>();
        tokio::spawn(async move {
            let mut wtxn = ENVIRONMENT.write_txn().unwrap();
            let database: Database<U128<byteorder::NativeEndian>, SerdeBincode<(i64, ByteSlice)>> = ENVIRONMENT.create_database(&mut wtxn, None).unwrap();
            let mut writes = 0;
            
            while let Some(msg) = rx.recv().await {
                match msg {
                    DbMessage::Write { id, data, added } => {
                        database.put(&mut wtxn, &id.as_u128(), &(added.unix_timestamp(), &data)).expect("Failed a write");
                        writes += 1;
                        if writes >= 10 {
                            wtxn.commit().unwrap();
                            wtxn = ENVIRONMENT.write_txn().unwrap();
                            writes = 0;
                        }
                    }
                    
                    DbMessage::Read { id, res } => {
                        let rtxn = ENVIRONMENT.read_txn().unwrap();
                        let ret = database.get(&rtxn, &id.as_u128()).unwrap().unwrap();
                        res.send(Bytes::copy_from_slice(ret)).unwrap();
                    }
                };
            };
    
        });
        
        Self {
            tx
        }
    }
}