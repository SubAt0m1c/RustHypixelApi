use std::{fs, sync::LazyLock, time::{Duration, Instant}};

use actix_web::{cookie::time::UtcDateTime, web::Bytes};
use heed::{Database, Env, EnvOpenOptions, Error, byteorder, types::{Bytes as ByteSlice, U128}};
use tokio::{sync::{mpsc::{UnboundedSender, unbounded_channel}, oneshot::{self, error::RecvError}}, time::interval};
use uuid::Uuid;

use crate::{cache::database::{batch_state::BatchState, db_entry::DbEntry, db_message::DbMessage}, routes::profile::PROFILE_DB_TTL};

static ENVIRONMENT: LazyLock<Env> = LazyLock::new(|| {
    fs::create_dir_all(".db").unwrap();
    unsafe { 
        EnvOpenOptions::new()
            .map_size(4 * 1024 * 1024 * 1024) //4gb size
            .open(".db") 
    }.unwrap()
});

const PAUSE_TIME: Duration = Duration::from_millis(50);
const MAX_AGE_TIME: Duration = Duration::from_millis(200);
const MAX_BATCH_SIZE: usize = 20;
const TIMER_INTERVAL: Duration = Duration::from_millis(25);

#[derive(Clone)]
pub struct DbHandle {
    tx: UnboundedSender<DbMessage>
}

impl DbHandle {
    pub fn new() -> Self {
        let (tx, mut rx) = unbounded_channel::<DbMessage>();
        
        tokio::spawn(async move {
            let mut wtxn = ENVIRONMENT.write_txn().expect("Failed to get write txn init");
            let database: Database<U128<byteorder::NativeEndian>, ByteSlice> = ENVIRONMENT.create_database(&mut wtxn, None).unwrap();
            wtxn.commit().expect("Should have committed initial db creation");
            
            let mut batch_state = BatchState::new(MAX_BATCH_SIZE);

            let mut timer = interval(TIMER_INTERVAL);

            loop {
                tokio::select! {
                    recv_opt = rx.recv() => {
                        let Some(recv) = recv_opt else {
                            eprintln!("BOO! (fatal) recv/sender being dropped should never happen.");
                            break;
                        }; 

                        match recv {
                            DbMessage::Write { id, data } => {
                                let now = Instant::now();
                                
                                if batch_state.start_time.is_none() {
                                    batch_state.start_time = Some(now);
                                }
        
                                batch_state.last_write_time = Some(now);
                                batch_state.pending_writes.push((id, data.bytes(), UtcDateTime::now()));
                               
                                if batch_state.pending_writes.len() >= MAX_BATCH_SIZE {
                                    if let Err(e) = commit_batch(&mut batch_state, database) {
                                        eprintln!("Error committing batch: {e}");
                                        batch_state.pending_writes.clear();
                                        batch_state.start_time = None;
                                        batch_state.last_write_time = None;
                                    }
                                }
                            }
                            DbMessage::Read { id, res } => {
                                let rtxn = ENVIRONMENT.read_txn().expect("Should have gotten read txn!");
                                let ret = database.get(&rtxn, &id.as_u128()).expect("Should have gotten response from db");
                                match ret {
                                    Some(data) => {
                                        let (time, bytes) = DbEntry::deconstruct_slice(data);
                                        if UtcDateTime::now() - time >= Duration::from_secs(*PROFILE_DB_TTL) {
                                            res.send(None).unwrap_or_else(|_| eprintln!("Should have successfully sent response!"));
                                        } else {
                                            res.send(Some(Bytes::copy_from_slice(bytes))).unwrap_or_else(|_| eprintln!("Should have successfully sent response!"));
                                        }
                                    }
                                    None => { res.send(None).unwrap_or_else(|_| eprintln!("Should have successfully sent response!")); }
                                }
                            }
                        }
                    }

                    _ = timer.tick() => {
                        let now = Instant::now();

                        let since_batch_start = batch_state.start_time.map_or(Duration::ZERO, |start| now.duration_since(start));
                        let since_last_write = batch_state.last_write_time.map_or(Duration::ZERO, |last| now.duration_since(last));
                        
                        if since_last_write >= PAUSE_TIME || since_batch_start >= MAX_AGE_TIME {
                            if let Err(e) = commit_batch(&mut batch_state, database) {
                                eprintln!("Error committing batch: {e}");
                                batch_state.pending_writes.clear();
                                batch_state.start_time = None;
                                batch_state.last_write_time = None;
                            }
                        }
                    }
                }
            } 
        });
        
        Self {
            tx
        }
    }

    pub fn write(&self, id: Uuid, data: DbEntry) {
        let message = DbMessage::Write { id, data};
        self.tx.send(message).expect("DB channel shouldnt close");
    }

    pub async fn read(&self, id: Uuid) -> Result<Option<Bytes>, RecvError> {
        let (tx, rx) = oneshot::channel();
        let message = DbMessage::Read { id, res: tx };
        self.tx.send(message).expect("DB channel shouldnt close");
        rx.await
    }
}

fn commit_batch(state: &mut BatchState, db: Database<U128<byteorder::NativeEndian>, ByteSlice>) -> Result<(), Error>{
    if state.pending_writes.is_empty() {
        return Ok(())
    }

    let mut wtxn = ENVIRONMENT.write_txn()?;
    
    for (id, data, _) in state.pending_writes.drain(..).into_iter() {
        db.put(&mut wtxn, &id.as_u128(), &data)?;
    }
    wtxn.commit()?;

    state.start_time = None;
    state.last_write_time = None;

    Ok(())
}