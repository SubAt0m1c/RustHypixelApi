use std::{env, fs, io, sync::LazyLock, time::{Duration, Instant}};

use actix_web::{web::Bytes};
use heed::{Database, Env, EnvOpenOptions, byteorder, types::{Bytes as ByteSlice, U128}};
use tokio::{sync::{mpsc::{UnboundedSender, unbounded_channel}, oneshot}, task::JoinHandle};
use uuid::Uuid;

use crate::{cache::{compression::extract_data, database::{batch_state::{BatchState, WriteType}, db_message::DbMessage, timed_queue::TimedQueue}}, routes::profile::PROFILE_DB_TTL};

static DB_SIZE: LazyLock<usize> = LazyLock::new(|| {
    let size = env::var("DB_SIZE");
    match size {
        Ok(size) => {
            size.parse().expect("DB_SIZE should be a usize (likely u64, could be u32)!")
        }
        Err(e) => {
            eprintln!("{e}: DB_SIZE, using 4096 (mb) (4 gb) default.");
            4096
        }
    }
});

// this doesnt really need to be lazylock if we just move it into the db handle... shrug
static ENVIRONMENT: LazyLock<Env> = LazyLock::new(|| {
    let path = ".db";
    if fs::exists(path).expect("Shouldnt have failed to verify path directory's existence.") {
        fs::remove_dir_all(path).expect("Shouldnt have failed to remove the path directory.");
    } // wiping on rs to keep consistency with the memory based ttl mechanism.
    fs::create_dir_all(path).expect("Shouldnt have failed to create db directory.");

    unsafe { 
        EnvOpenOptions::new()
            .map_size(*DB_SIZE * 1024 * 1024)
            .open(path)
    }.unwrap()
});

pub struct DbHandle {
    tx: UnboundedSender<DbMessage>,
    handle: JoinHandle<()>
}

impl DbHandle {
    pub fn new() -> Self {
        let (tx, mut rx) = unbounded_channel::<DbMessage>();
        
        let handle = tokio::spawn(async move {
            let mut wtxn = ENVIRONMENT.write_txn().expect("Failed to get write txn init");
            let database: Database<U128<byteorder::NativeEndian>, ByteSlice> = ENVIRONMENT.create_database(&mut wtxn, None).unwrap();
            wtxn.commit().expect("Should have committed initial db creation");
            
            let mut batch_state = BatchState::new();
            let mut ttl_queue: TimedQueue<Uuid> = TimedQueue::new();

            loop {
                tokio::select! {
                    recv_opt = rx.recv() => {
                        let Some(recv) = recv_opt else {
                            eprintln!("BOO! (fatal) recv/sender being dropped should never happen.");
                            break;
                        }; 

                        match recv {
                            DbMessage::Write { id, data } => {
                                batch_state.insert(id, WriteType::Insert(data), &ENVIRONMENT, database);
                                ttl_queue.insert(id, Instant::now() + Duration::from_secs(*PROFILE_DB_TTL))
                            }
                            DbMessage::Read { id, res } => {
                                let rtxn = ENVIRONMENT.read_txn().expect("Should have gotten read txn!");
                                let ret = database.get(&rtxn, &id.as_u128()).expect("Should have gotten response from db");
                                res.send(ret.map(Bytes::copy_from_slice)).expect("Reciever shouldnt have been dropped!");
                            }
                        }
                    }

                    id = ttl_queue.recv() => {
                        batch_state.insert(id, WriteType::Delete, &ENVIRONMENT, database);
                    }
                    
                    _ = batch_state.wait_for_commit() => {
                        batch_state.commit_checked(&ENVIRONMENT, database);
                    }
                }
            } 
        });
        
        Self {
            tx,
            handle
        }
    }

    pub fn write(&self, id: Uuid, data: Bytes) {
        self.tx.send(DbMessage::Write { id, data }).expect("DB channel shouldnt close");
    }

    pub async fn read(&self, id: Uuid) -> Result<Option<Bytes>, io::Error> {
        let (tx, rx) = oneshot::channel();
        self.tx.send(DbMessage::Read { id, res: tx }).expect("DB channel shouldnt close");
        let Some(data) = rx.await.map_err(io::Error::other)? else {
            return Ok(None)
        };

        Ok(Some(Bytes::from(extract_data(&data).map_err(io::Error::other)?)))
    }
}

impl Drop for DbHandle {
    fn drop(&mut self) {
        self.handle.abort();
    }
}