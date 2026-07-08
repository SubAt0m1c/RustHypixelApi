use std::{collections::BinaryHeap, future::poll_fn, sync::Arc, time::Duration};
use flume::{Receiver, TryRecvError};
use futures_timer::Delay;
use futures_util::{FutureExt, StreamExt, TryFutureExt, stream::FuturesUnordered};

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
    par_key: ParKey,
    retries: u64,
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


pub(crate) async fn run_expiration_task<RT: SendRuntime>(cache_inner: Arc<DbInner<RT>>, rx: Receiver<ExpCMD>) {    
    let mut heap: BinaryHeap<QueueEntry> = BinaryHeap::new();
    let mut pending_deletions = FuturesUnordered::new();
    
    loop {
        loop {
            match rx.try_recv() {
                Ok(msg) => handle_message(msg, &mut heap),
                Err(TryRecvError::Empty) => break,
                Err(TryRecvError::Disconnected) => panic!("All handles to the expiration task should not be dropped!")
            }
        }

        if heap.is_empty() {
            match rx.recv_async().await {
                Ok(msg) => handle_message(msg, &mut heap),
                Err(_) => panic!("All handles to the expiration task should not be dropped!"),
            }
            continue;
        }

        let next = heap.peek().expect("Should have verified heap is not empty").time;
        let now = unix_secs();

        if next <= now {
            while let Some(entry) = heap.peek() {
                if entry.time > now { break }

                let entry = heap.pop().expect("Should have verified heap.peak() isn't None");
                let future = cache_inner.partitions.get_ref(entry.par_key).purge::<RT>(&cache_inner.entries);
                pending_deletions.push(future.map_err(|e| (entry, e)));
            }
            continue;
        }
        
        let duration_until_wake = Duration::from_secs(next.saturating_sub(now));
        let sleep = Delay::new(duration_until_wake);

        let next_completion = poll_fn(|cx| {
            if pending_deletions.is_empty() { std::task::Poll::Pending } else { pending_deletions.poll_next_unpin(cx) }
        }); // we need to make polling while its empty not return Poll::Ready(None), otherwise it will hog the select.
        
        futures_util::select! {
            res = next_completion.fuse() => {
                let Some(Err((entry, err))) = res else { continue };
                eprintln!("Failed to delete partition: {err}");

                const INITIAL_BACKOFF: u64 = 5;     // 5 seconds
                const MAX_BACKOFF: u64 = 60 * 60;   // 1 hour

                let retries = entry.retries.saturating_add(1);
                let delay = INITIAL_BACKOFF << retries;
                if delay > MAX_BACKOFF { continue }; // we just give up here. it likely wont succeed any future tries. (stale keys have already been removed)

                heap.push(QueueEntry { time: now + delay, retries, ..entry }); // now shouldn't be too stale to use here
            } // we just need to poll all the futures, they already run the code they need to internally.

            res = rx.recv_async().fuse() => handle_message(res.expect("All handles to the expiration task should not be dropped!"), &mut heap),
                
            _ = sleep.fuse() => continue, // we loop back, which will shortly purge the woken entry.
        };
    }
}


fn handle_message(msg: ExpCMD, queue: &mut BinaryHeap<QueueEntry>) {
    match msg {
        ExpCMD::Schedule { time, par_key } => queue.push(QueueEntry { time, par_key, retries: 0 }),
    }
}