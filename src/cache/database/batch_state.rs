use std::time::{Duration, Instant};

use heed::{Database, Env, Error, byteorder, types::{Bytes as ByteSlice, U128}};
use tokio::{sync::Notify, time::sleep_until};

use crate::cache::{UuidKey, database::CompressedBytes};

const MAX_BATCH_SIZE: usize = 20;
const PAUSE_TIME: Duration = Duration::from_millis(50);
const MAX_AGE_TIME: Duration = Duration::from_millis(200);

pub enum WriteType {
    Insert(CompressedBytes),
    Delete,
}

pub struct BatchState {
    pub pending_writes: Vec<(UuidKey, WriteType)>,
    pub start_time: Option<Instant>,
    pub last_write_time: Option<Instant>,
    pub waker: Notify
}

impl BatchState {
    pub fn new() -> Self {
        BatchState {
            pending_writes: Vec::with_capacity(MAX_BATCH_SIZE), //todo: max batch size parameter
            start_time: None,
            last_write_time: None,
            waker: Notify::new()
        }
    }
    
    pub fn insert(&mut self, id: UuidKey, write_type: WriteType, env: &Env, db: Database<U128<byteorder::NativeEndian>, ByteSlice>) {
        self.pending_writes.push((id, write_type));
        self.update_time();
       
        if self.pending_writes.len() >= MAX_BATCH_SIZE {
            self.commit_checked(env, db);
        }
    }

    pub fn commit_checked(&mut self, env: &Env, db: Database<U128<byteorder::NativeEndian>, ByteSlice>) {
        if let Err(e) = self.commit(env, db) {
            eprintln!("Error committing batch: {e}");
            self.pending_writes.clear();
            self.start_time = None;
            self.last_write_time = None;
            self.waker.notify_one();
        }
    }

    pub async fn wait_to_commit(&self) {
        loop {
            let notified = self.waker.notified();
            let Some(start) = self.start_time else {
                notified.await;
                continue;
            };
    
            let pause_deadline = self.last_write_time.unwrap_or(start) + PAUSE_TIME;
            let age_deadline = start + MAX_AGE_TIME;
            let deadline = pause_deadline.min(age_deadline);
        
            tokio::select! {
                _ = sleep_until(deadline.into()) => {
                    return
                }
                _ = notified => {
                    continue
                }
            }
        }
    }
    
    fn update_time(&mut self) {
        let now = Instant::now();
        if self.start_time.is_none() {
            self.start_time = Some(now);
        }

        self.last_write_time = Some(now);
        self.waker.notify_one();
    }

    fn commit(&mut self, env: &Env, db: Database<U128<byteorder::NativeEndian>, ByteSlice>) -> Result<(), Error> {
        if self.pending_writes.is_empty() {
            return Ok(())
        }

        let mut wtxn = env.write_txn()?;

        for (id, write_type) in self.pending_writes.drain(..).into_iter() {
            let key = id.as_u128();
            match write_type {
                WriteType::Insert(data) => {
                    db.put(&mut wtxn, &key, &data)?;
                }
                WriteType::Delete => {
                    db.delete(&mut wtxn, &key)?;
                }
            }
        }
        wtxn.commit()?;
        
        self.start_time = None;
        self.last_write_time = None;
        self.waker.notify_one();
    
        Ok(())
    }
}