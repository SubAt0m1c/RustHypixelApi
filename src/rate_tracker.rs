use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};
use tokio::sync::RwLock;

pub struct RateTracker {
    requests: AtomicU64,
    restart_time: RwLock<Instant>,
}

impl RateTracker {
    pub fn new() -> Self {
        RateTracker {
            requests: AtomicU64::new(0),
            restart_time: RwLock::new(Instant::now()),
        }
    }

    pub fn requests(&self) -> u64 {
        self.requests.load(Ordering::Relaxed)
    }

    pub async fn elapsed(&self) -> Duration {
        self.restart_time.read().await.elapsed()
    }

    pub async fn inc(&self, start_time: &Instant) {
        let read_time = self.restart_time.read().await;
        let elapsed = start_time.duration_since(*read_time).as_secs();
        if elapsed > 300 {
            drop(read_time); // read_time needs to be dropped here or reset's .write() would hang.
            self.reset(start_time).await;
        }

        self.requests.fetch_add(1, Ordering::Relaxed);
    }

    async fn reset(&self, start_time: &Instant) {
        self.requests.store(0, Ordering::Relaxed);
        let mut time = self.restart_time.write().await;
        *time = *start_time;
    }
}
