use std::{collections::BinaryHeap, sync::Arc, time::Duration};
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


pub(crate) fn spawn_expiration_task<RT: SendRuntime>(cache_inner: Arc<DbInner<RT>>, rx: Receiver<ExpCMD>) {    
    RT::spawn(async move {
        let mut heap: BinaryHeap<QueueEntry> = BinaryHeap::new();
        let mut pending_deletions = FuturesUnordered::new();

        loop {
            loop {
                match rx.try_recv() {
                    Ok(msg) => handle_message(msg, &mut heap),
                    Err(TryRecvError::Empty) => break,
                    Err(TryRecvError::Disconnected) => return
                }
            }

            if heap.is_empty() {
                match rx.recv_async().await {
                    Ok(msg) => handle_message(msg, &mut heap),
                    Err(_) => return,
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
            }
            
            let duration_until_wake = Duration::from_secs(next.saturating_sub(now));
            let sleep = Delay::new(duration_until_wake);

            let poll = poll_and_apply(&mut pending_deletions, &mut heap, |res, heap| {
                let Err((entry, err)) = res else { return };
                eprintln!("Failed to delete partition: {err}");

                const INITIAL_BACKOFF: u64 = 5;     // 5 seconds
                const MAX_BACKOFF: u64 = 60 * 60;   // 1 hour

                let retries = entry.retries.saturating_add(1);
                let delay = INITIAL_BACKOFF << retries;
                if delay > MAX_BACKOFF { return };

                heap.push(QueueEntry { time: now + delay, retries, ..entry });
            });

            futures_util::select! {
                _ = poll.fuse() => {} // we just need to poll all the futures, they already run the code they need to internally.

                res = rx.recv_async().fuse() => {
                    handle_message(res.expect("All handles to the expiration task should not be dropped!"), &mut heap);
                }

                _ = sleep.fuse() => continue, // we loop back, which will shortly purge the woken entry.
            };
        }
    });
}


fn handle_message(msg: ExpCMD, queue: &mut BinaryHeap<QueueEntry>) {
    match msg {
        ExpCMD::Schedule { time, par_key } => queue.push(QueueEntry { time, par_key, retries: 0 }),
    }
}

async fn poll_and_apply<F: Future<Output = R>, R, V>(futures: &mut FuturesUnordered<F>, v: &mut V, apply: impl Fn(R, &mut V) -> ()) {
    while let Some(future) = futures.next().await {
        apply(future, v)
    }
}