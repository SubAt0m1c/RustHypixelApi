#![allow(clippy::items_after_statements)]

use std::{cmp::Reverse, collections::BinaryHeap, future::poll_fn, sync::Arc, time::Duration};

use flume::{Receiver, TryRecvError};
use futures_util::{FutureExt, StreamExt, TryFutureExt, future::{Either, err}, select, stream::FuturesUnordered};

use crate::{Error, Result, db::{Maps, ViableHasher}, runtime::Runtime, unix_secs};

pub(crate) enum ExpCMD {
    Schedule {
        time: u64,
        par_key: usize,
    }
}

#[derive(PartialEq, Eq)]
pub(crate) struct QueueEntry {
    time: u64,
    par_key: usize,
    retries: u64,
}

impl Ord for QueueEntry {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.time.cmp(&other.time)
    }
}

impl PartialOrd for QueueEntry {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}


pub(crate) async fn run_expiration_task<RT: Runtime, S: ViableHasher>(db_maps: Arc<Maps<S>>, rx: Receiver<ExpCMD>) {    
    let mut heap: BinaryHeap<Reverse<QueueEntry>> = BinaryHeap::new();
    let mut pending_deletions = FuturesUnordered::new();
    
    'outer: loop {
        loop {
            match rx.try_recv() {
                Ok(msg) => handle_message(msg, &mut heap),
                Err(TryRecvError::Empty) => break,
                Err(TryRecvError::Disconnected) => break 'outer,
            }
        }

        if heap.is_empty() {
            match rx.recv_async().await {
                Ok(msg) => handle_message(msg, &mut heap),
                Err(_) => break 'outer,
            }
            continue 'outer;
        }
        
        let now = unix_secs();
        while let Some(entry) = get_next(&mut heap, now) {
            pending_deletions.push(purge_partition::<RT, S>(&db_maps, entry.par_key).map_err(|e| (entry, e)));
        }

        let next_completion = poll_fn(|cx| {
            if pending_deletions.is_empty() { std::task::Poll::Pending } else { pending_deletions.poll_next_unpin(cx) }
        }); // we need to make polling while its empty not return Poll::Ready(None), otherwise it will hog the select.
        
        select! {
            res = next_completion.fuse() => {
                let Some(Err((entry, err))) = res else { continue 'outer }; // if its an error, it was successfully deleted and we don't care about it anymore.
                eprintln!("Failed to delete partition: {err}");

                const INITIAL_BACKOFF: u64 = 5;     // 5 seconds
                const MAX_BACKOFF: u64 = 60 * 60;   // 1 hour

                let retries = entry.retries.saturating_add(1);
                let delay = INITIAL_BACKOFF << retries.max(u64::from(u64::BITS));
                if delay > MAX_BACKOFF { continue 'outer } // we just give up here. it likely wont succeed any future tries. (stale keys have already been removed)

                heap.push(Reverse(QueueEntry { time: now + delay, retries, ..entry })); // now being stale here doesnt really matter
            }

            res = rx.recv_async().fuse() => {
                match res {
                    Ok(msg) => handle_message(msg, &mut heap),
                    Err(_) => break 'outer,
                }
            }

            () = sleep_until_next::<RT>(&mut heap, now).fuse() => {}, // we loop back, which will shortly purge the woken entry.
        };
    }
    // database closed due to it being dropped. 
}

#[allow(clippy::needless_pass_by_value)]
fn handle_message(msg: ExpCMD, queue: &mut BinaryHeap<Reverse<QueueEntry>>) {
    match msg {
        ExpCMD::Schedule { time, par_key } => queue.push(Reverse(QueueEntry { time, par_key, retries: 0 })),
    }
}

fn get_next(heap: &mut BinaryHeap<Reverse<QueueEntry>>, now: u64) -> Option<QueueEntry> {
    if heap.peek().is_none_or(|Reverse(e)| e.time > now) { return None }
    heap.pop().map(|Reverse(e)| e)
}

async fn sleep_until_next<RT: Runtime>(heap: &mut BinaryHeap<Reverse<QueueEntry>>, now: u64) {
    let sleep_duration = match heap.peek() {
        Some(Reverse(next_entry)) => Duration::from_secs(next_entry.time.saturating_sub(now)),
        None => Duration::MAX, // we sleep forever so we can sit in a select loop and not hog anything.
    };
    RT::sleep(sleep_duration).await;
}

#[must_use = "This future has side effects before being polled!"]
fn purge_partition<RT: Runtime, S: ViableHasher>(maps: &Maps<S>, key: usize) -> impl Future<Output = Result<()>> + use<RT, S> {
    let Some(partition) = maps.partitions.get(key) else {
        return Either::Left(err(Error::PARTITION_NOT_FOUND)); // either because i want to return errors on the future itself
    };

    maps.partitions.remove(key); // sharded_slab has no problem letting us keep a reference while marking it to be deleted.
    Either::Right(partition.purge::<RT, S>(&maps.entries))
}