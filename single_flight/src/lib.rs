//! Asyncronous concurrent single flight request deduplication.
//! Runtime agnostic, built for high concurrency.
//! 
//! Heavily based on [async_singleflight](https://github.com/PureWhiteWu/async_singleflight) under the MIT license.
//! Other crates did not offer the mostly lock-free design I was looking for.

mod types;
mod group;
mod error;

pub use group::*;

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::time::Duration;
    use tokio::sync::oneshot;

    #[allow(clippy::unused_async)]
    async fn return_res() -> Result<usize, ()> {
        Ok(7)
    }

    async fn expensive_fn<const RES: usize>(delay: u64) -> Result<usize, ()> {
        tokio::time::sleep(Duration::from_millis(delay)).await;
        Ok(RES)
    }

    #[tokio::test]
    async fn test_simple() {
        let g = DefaultGroup::new();
        let res = g.work("key", return_res()).await;
        let r = res.unwrap();
        assert_eq!(r, 7);
    }

    #[tokio::test]
    async fn test_multiple_threads() {
        use std::sync::Arc;

        use futures::future::join_all;

        let g = Arc::new(DefaultGroup::new());
        let mut handlers = Vec::with_capacity(10);
        for _ in 0..10 {
            let g = g.clone();
            handlers.push(tokio::spawn(async move {
                let res = g.work("key", expensive_fn::<7>(300)).await;
                let r = res.unwrap();
                println!("{r}");
            }));
        }

        join_all(handlers).await;
    }

    #[tokio::test]
    async fn test_multiple_threads_custom_type() {
        use std::sync::Arc;

        use futures::future::join_all;

        let g = Arc::new(Group::<u64, usize, ()>::new());
        let mut handlers = Vec::with_capacity(10);
        for _ in 0..10 {
            let g = g.clone();
            handlers.push(tokio::spawn(async move {
                let res = g.work(&42, expensive_fn::<8>(300)).await;
                let r = res.unwrap();
                println!("{r}");
            }));
        }

        join_all(handlers).await;
    }

    #[tokio::test]
    async fn test_drop_leader() {
        let group = Arc::new(DefaultGroup::new());

        // Signal when the leader's inner future gets polled (implies map entry inserted).
        let (ready_tx, ready_rx) = oneshot::channel::<()>();

        let leader_owned = group.clone();
        let leader = tokio::spawn(async move {
            // The inner future signals on first poll, then sleeps long.
            let fut = async move {
                let _ = ready_tx.send(());
                tokio::time::sleep(Duration::from_millis(500)).await;
                Ok::<usize, ()>(7)
            };
            // We expect this task to be aborted before completion.
            let _ = leader_owned.work("key", fut).await;
        });

        // Wait until the leader's future has been polled once (map entry is in place).
        let _ = ready_rx.await;

        // Spawn a follower that will wait on the existing key and should observe LeaderDropped.
        let follower_owned = group.clone();
        let follower = tokio::spawn(async move {
            follower_owned
                .work("key", async { Ok::<usize, ()>(42) })
                .await
        });

        // Give the follower a chance to attach to the receiver.
        tokio::task::yield_now().await;

        // Abort the leader to trigger LeaderDropped notification to all followers.
        leader.abort();

        // The follower should return LeaderDropped.
        let res = tokio::time::timeout(Duration::from_secs(1), follower)
            .await
            .expect("follower should finish in time")
            .expect("follower task should not panic");

        assert_eq!(res, Ok(42));
    }

    /// Regression test for issue #12: when the leader is dropped, only ONE follower
    /// should become the new leader. Due to a race condition in the `LeaderDropped`
    /// handler (which calls `map.remove(key)` and can delete a newly-inserted entry
    /// from a follower that already became the new leader), multiple followers can
    /// each independently become leaders and execute the work function.
    ///
    /// The race scenario:
    /// 1. Leader is dropped; followers A and B both see `LeaderDropped`
    /// 2. Follower A acquires the lock and calls remove(key), then loops back
    ///    into `work_inner`where it inserts a NEW entry and becomes the new leader
    /// 3. Follower B acquires the lock for its remove(key) call -- but by now
    ///    the map entry belongs to the new leader (A). B removes it anyway!
    /// 4. Follower B loops back into `work_inner`, sees no entry, inserts its own,
    ///    and becomes a SECOND independent leader.
    ///
    /// We use a multi-threaded runtime and many iterations to trigger this race.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn test_leader_drop_single_new_leader() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        use tokio::sync::Barrier;

        const NUM_FOLLOWERS: usize = 5;

        // Run the test many times to increase the chance of hitting the race.
        for iteration in 0..200 {
            let group = Arc::new(DefaultGroup::new());

            // Counts how many times the actual work function body executes.
            let execute_count = Arc::new(AtomicUsize::new(0));

            // Signal when the leader's inner future gets polled (map entry inserted).
            let (leader_ready_tx, leader_ready_rx) = oneshot::channel::<()>();

            // Barrier: all followers + main thread wait here to sync up before
            // we abort the leader, ensuring followers are subscribed.
            let barrier = Arc::new(Barrier::new(NUM_FOLLOWERS + 1));

            // Spawn the leader task.
            let leader_group = group.clone();
            let leader = tokio::spawn(async move {
                let fut = async move {
                    let _ = leader_ready_tx.send(());
                    tokio::time::sleep(Duration::from_mins(1)).await;
                    Ok::<usize, ()>(999)
                };
                let _ = leader_group.work("key", fut).await;
            });

            // Wait for the leader's future to be polled (entry in the map).
            let _ = leader_ready_rx.await;

            let mut follower_handles = Vec::with_capacity(NUM_FOLLOWERS);

            for _ in 0..NUM_FOLLOWERS {
                let g = group.clone();
                let cnt = execute_count.clone();
                let b = barrier.clone();
                follower_handles.push(tokio::spawn(async move {
                    // Strategy: each follower signals readiness via the barrier,
                    // then calls work() which subscribes to the leader's channel.
                    // When the leader is aborted, followers see LeaderDropped and
                    // retry via the work() loop. Only one should become the new
                    // leader; the rest should subscribe to the new leader's channel.
                    b.wait().await;

                    g.work("key", async move {
                        cnt.fetch_add(1, Ordering::SeqCst);
                        // Yield to give other followers a chance to also become
                        // leaders if the race condition is triggered.
                        tokio::task::yield_now().await;
                        Ok::<usize, ()>(42)
                    })
                    .await
                }));
            }

            // Wait for all followers to be ready to enter work.
            barrier.wait().await;

            // Give followers time to actually enter work_inner and subscribe
            // as receivers on the watch channel.
            tokio::time::sleep(Duration::from_millis(5)).await;

            // Abort the leader. This triggers LeaderDropped to all followers.
            leader.abort();

            // Wait for all followers to complete.
            for handle in follower_handles {
                let res = tokio::time::timeout(Duration::from_secs(5), handle)
                    .await
                    .expect("follower should finish in time")
                    .expect("follower task should not panic");
                assert_eq!(res, Ok(42), "follower should get the correct result");
            }

            // The critical assertion: the work function should have executed exactly
            // once (by the single new leader). If the bug is present, multiple
            // followers become independent leaders and execute_count will be > 1.
            let count = execute_count.load(Ordering::SeqCst);
            assert_eq!(
                count, 1,
                "Iteration {iteration}: Expected exactly 1 work execution after leader drop, \
                 but got {count}. This indicates multiple followers became leaders (issue #12)."
            );
        }
    }
   

    /// After a promoted leader is dropped, it should properly remove its entries.
    #[tokio::test]
    async fn test_stale() {
        let group = Arc::new(DefaultGroup::new());

        let (leader_ready_tx, leader_ready_rx) = oneshot::channel::<()>();
        let leader_group = group.clone();
        let leader = tokio::spawn(async move {
            let _ = leader_group
                .work("key", async move {
                    let _ = leader_ready_tx.send(());
                    tokio::time::sleep(Duration::from_mins(1)).await;
                    Ok::<usize, ()>(999)
                })
                .await;
        });
        let _ = leader_ready_rx.await;

        tokio::task::yield_now().await;

        leader.abort();

        // throw it back to the runtime so it can hit an await and get dropped.
        tokio::task::yield_now().await;

        assert!(!group.check_if_stale("key"));
    }

}