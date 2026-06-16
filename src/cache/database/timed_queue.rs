use std::{collections::BinaryHeap, time::Instant};

use tokio::{sync::Notify, time::sleep_until};

#[derive(PartialEq, Eq, PartialOrd)]
struct Entry<T: Ord> {
    deadline: Instant,
    item: T,
}

impl<T:Ord> Ord for Entry<T> {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        other.deadline.cmp(&self.deadline)
    }
}

pub struct TimedQueue<T: Ord> {
    heap: BinaryHeap<Entry<T>>,
    wake: Notify,
}

impl<T: Ord> TimedQueue<T> {
    pub fn new() -> Self {
        Self {
            heap: BinaryHeap::new(),
            wake: Notify::new(),
        }
    }

    pub fn insert(&mut self, item: T, deadline: Instant) {
        self.heap.push(Entry { deadline, item });
        self.wake.notify_one();
    }
    
    pub async fn recv(&mut self) -> T {
        loop {
            let notified = self.wake.notified(); // weird race condition fix
            let Some(deadline) = self.heap.peek().map(|next| next.deadline) else { 
                notified.await;
                continue;
            };

            tokio::select! {
                _ = sleep_until(deadline.into()) => {
                    return self.heap.pop().expect("Top of heap should have been verified to exist still.").item;
                }
                _ = notified => {
                    continue
                }
            }
        }
    }
}