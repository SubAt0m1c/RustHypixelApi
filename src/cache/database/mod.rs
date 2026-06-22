use std::{fs, sync::LazyLock, time::{Duration, Instant}};

use actix_web::web::Bytes;
use heed::{byteorder, types::{Bytes as ByteSlice, U128}, Database, Env, EnvOpenOptions};
use tokio::{sync::mpsc::UnboundedReceiver, task::JoinHandle};
use uuid::Uuid;

use crate::{cache::database::{batch_state::{BatchState, WriteType}, db_message::DbMessage, timed_queue::TimedQueue}, request_utils::env_var, routes::profile::PROFILE_DB_TTL};

mod batch_state;
pub mod db_handle;
pub mod db_message;
mod timed_queue;

static DB_SIZE: LazyLock<usize> = LazyLock::new(|| env_var("DB_SIZE", 4096));

// this doesnt really need to be lazylock if we just move it into the db handle... shrug
pub(crate) static ENVIRONMENT: LazyLock<Env> = LazyLock::new(|| {
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

pub fn spawn_db_worker(mut rx: UnboundedReceiver<DbMessage>) -> JoinHandle<()> {
    tokio::spawn(async move {
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
                
                _ = batch_state.wait_to_commit() => {
                    batch_state.commit_checked(&ENVIRONMENT, database);
                }
            }
        } 
    })
}

/// Wrapper for the compressed data that gets written to the db.
/// Currently a Vec<u8> because compression returns this directly and we have 0 clones of it.
/// If, at any point, a clone is needed, this should be changed to a Bytes. 
/// Vec<u8> to Bytes conversion is trivial, but it's unnecessary without clones.
pub(crate) type CompressedBytes = Vec<u8>;