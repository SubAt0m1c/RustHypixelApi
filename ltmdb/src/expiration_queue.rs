#![allow(clippy::items_after_statements)]

use std::{cmp::Reverse, collections::BinaryHeap, future::poll_fn, time::Duration};
use flume::{Receiver, TryRecvError};
use futures_timer::Delay;
use futures_util::{FutureExt, StreamExt, TryFutureExt, stream::FuturesUnordered};

use crate::{db::DbView, runtime::Runtime, unix_secs};

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


pub(crate) async fn run_expiration_task<RT: Runtime>(db_view: DbView<RT>, rx: Receiver<ExpCMD>) {    
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

        let Reverse(next_entry) = heap.peek().expect("Should have verified heap is not empty");
        let now = unix_secs();
        
        if next_entry.time <= now {
            while let Some(Reverse(entry)) = heap.peek() {
                if entry.time > now { break }

                let Reverse(entry) = heap.pop().expect("Should have verified heap.peak() isn't None");
                pending_deletions.push(db_view.purge_partition(entry.par_key).map_err(|e| (entry, e)));
            }
        }

        // Sleeping forever enables continueing into the select, so existing pending deletions can be processed. The select will wake up when a new entry is scheduled.
        let sleep = Delay::new(heap.peek().map_or(Duration::MAX, |Reverse(entry)| Duration::from_secs(entry.time.saturating_sub(now))));

        let next_completion = poll_fn(|cx| {
            if pending_deletions.is_empty() { std::task::Poll::Pending } else { pending_deletions.poll_next_unpin(cx) }
        }); // we need to make polling while its empty not return Poll::Ready(None), otherwise it will hog the select.
        
        futures_util::select! {
            res = next_completion.fuse() => {
                let Some(Err((entry, err))) = res else { continue 'outer }; // if its an error, it was successfully deleted and we don't care about it anymore.
                eprintln!("Failed to delete partition: {err}");

                const INITIAL_BACKOFF: u64 = 5;     // 5 seconds
                const MAX_BACKOFF: u64 = 60 * 60;   // 1 hour

                let retries = entry.retries.saturating_add(1);
                let delay = INITIAL_BACKOFF << retries; // this can panic or have bad behavior if retries becomes large. Not an issue for now though.
                if delay > MAX_BACKOFF { continue 'outer } // we just give up here. it likely wont succeed any future tries. (stale keys have already been removed)

                heap.push(Reverse(QueueEntry { time: now + delay, retries, ..entry })); // now shouldn't be too stale to use here
            }

            res = rx.recv_async().fuse() => {
                match res {
                    Ok(msg) => handle_message(msg, &mut heap),
                    Err(_) => break 'outer,
                }
            }
                
            () = sleep.fuse() => {}, // we loop back, which will shortly purge the woken entry.
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