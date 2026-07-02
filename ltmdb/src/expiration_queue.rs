use std::{collections::BinaryHeap, sync::Arc, time::Duration};
use flume::{Receiver, TryRecvError};
use futures::FutureExt;

use crate::{db::{DbInner, ParKey}, runtime::SendRuntime, unix_secs};

pub(crate) enum ExpCMD {
    Schedule {
        time: u64,
        par_key: ParKey,
    }
}

#[derive(PartialEq, Eq)]
pub(crate) struct QueueEntry {
    time: u64,
    par_key: ParKey
}

impl Ord for QueueEntry {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        other.time.cmp(&self.time)
    }
}

impl PartialOrd for QueueEntry {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(other.time.cmp(&self.time))
    }
}


pub(crate) fn spawn_expiration_task<RT: SendRuntime>(cache_inner: Arc<DbInner<RT>>, rx: Receiver<ExpCMD>) {    
    RT::spawn(async move {
        let mut heap: BinaryHeap<QueueEntry> = BinaryHeap::new();
        loop {
            loop {
                match rx.try_recv() {
                    Ok(msg) => handle_message(msg, &mut heap),
                    Err(TryRecvError::Empty) => break,
                    Err(TryRecvError::Disconnected) => return, //todo: flush
                }
            }

            if heap.is_empty() {
                match rx.recv_async().await {
                    Ok(msg) => handle_message(msg, &mut heap),
                    Err(_) => return,
                }
                continue;
            }

            let next = heap.peek().expect("Should have verified queue has at least one entry").time;
            let now = unix_secs();

            if next <= now {
                while let Some(entry) = heap.peek() {
                    if entry.time > now {
                        break
                    }

                    let entry = heap.pop().expect("Should have just verified the heap is not empty");

                    cache_inner.partitions.get_ref(entry.par_key).purge::<RT>(&cache_inner.entries).await.expect("Failed to purge file.");
                }
            }
            
            let duration_until_wake = Duration::from_secs(next.saturating_sub(now));
            let sleep = futures_timer::Delay::new(duration_until_wake);

            futures::select! {
                res = rx.recv_async().fuse() => {
                    match res {
                        Ok(cmd) => handle_message(cmd, &mut heap),
                        Err(_) => break
                    }
                }

                _ = sleep.fuse() => continue, // we loop back, which will shortly purge the woken entry.
            };
        }
    });
}


fn handle_message(msg: ExpCMD, queue: &mut BinaryHeap<QueueEntry>) {
    match msg {
        ExpCMD::Schedule { time, par_key } => queue.push(QueueEntry { time, par_key, }),
    }
}