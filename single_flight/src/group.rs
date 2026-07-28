use std::{borrow::Borrow, fmt::{self, Debug}, hash::{BuildHasher, Hash, RandomState}, marker::PhantomData, pin::pin, sync::Arc};

use concurrent_cell::{Collector, Compute, Operation};
use papaya::HashMap;
use simple_defer::{Deferred, defer};

use crate::{error::GroupWorkError, types::{ComputeEscape, Flight, FlightType, Leader, State}};

pub struct Group<K, T, E, S = RandomState> {
    collector: Arc<Collector>, // Collector for the ConcurrentCells in the Flights.
    map: HashMap<K, Arc<Flight<T>>, S>,
    _marker: PhantomData<fn(E)>,
}

pub type DefaultGroup<T, E = ()> = Group<String, T, E>;

impl<K, T, E, S> Debug for Group<K, T, E, S> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Group").finish()
    }
}

impl<K, T, E, S> Default for Group<K, T, E, S>
where
    S: Default,
{
    fn default() -> Self {
        Self {
            collector: Arc::new(Collector::default()),
            map: HashMap::<K, Arc<Flight<T>>, S>::default(),
            _marker: PhantomData,
        }
    }
}

impl<K, T, E, S> Group<K, T, E, S>
where
    S: Default,
{
    #[must_use]
    pub fn new() -> Group<K, T, E, S> {
        Self::default()
    }
}

impl<K, T, E, S> Group<K, T, E, S>
where
    T: Clone,
    K: Hash + Eq,
    S: BuildHasher,
{
    /// Creates a new `Group` with the given hash builder.
    #[must_use]
    pub fn with_hasher(hash_builder: S) -> Group<K, T, E, S> {
        Self {
            collector: Arc::new(Collector::default()),
            map: HashMap::with_hasher(hash_builder),
            _marker: PhantomData,
        }
    }
    
    /// Executes the given function while ensuring only one is "in flight" at any given time.
    /// 
    /// Duplicate calls wait for the original call to complete and return the same value by cloning it.
    /// If the returned value is expensive, wrap it in an [`Arc`].
    /// 
    /// # Errors
    /// 
    /// If the function returns an `Err`
    /// - The leading caller will return `Err(Some(error))`
    /// - Non-leading callers will return `Err(None)`
    pub async fn work<Q, F>(&self, key: &Q, fut: F) -> Result<T, Option<E>>
    where
        Q: Hash + Eq + ?Sized + Send + Sync + ToOwned<Owned = K>,
        F: Future<Output = Result<T, E>> + Send,
        K: std::borrow::Borrow<Q>,
    {
        let mut fut_opt = Some(fut);
        match self.work_inner(key, &mut fut_opt).await {
            Ok(val) => Ok(val),
            Err(GroupWorkError::Error(err)) => Err(Some(err)),
            Err(GroupWorkError::LeaderFailed) => Err(None),
        }
    }
    
    async fn work_inner<Q, F>(&self, key: &Q, fut: &mut Option<F>) -> Result<T, GroupWorkError<E>>
    where 
        Q: Hash + Eq + ?Sized + Send + Sync + ToOwned<Owned = K>,
        F: Future<Output = Result<T, E>> + Send,
        K: Borrow<Q>,
    {   
        // this loop matters in the rare event of a flight being accessed while a previously 0 follower flight got dropped and is in the middle of clearing the map.
        'acquire: loop {
            // We get the flight at the time of the worker starting. This will be released when there are no more workers accessing it.
            let current_flight = self.map.pin().get_or_insert_with(key.to_owned(), || {
                let collector = self.collector.clone();
                Arc::new(Flight::with_collector(collector))
            }).clone();
            
            let mut follower_guard = None;
            'retry: loop {
                let flight_type = {
                    let pinned = current_flight.cell_pinned();

                    // The original sometimes purposefully leaves stale entries when an initial leader dropped.
                    // It does this so late retryers after a new leader was assigned can still access the old value 
                    // instead of starting new work. Due to this, it reasons about these late retryers vs stale entries
                    // and lets most paths start new leaders if they are a fresh worker.

                    // We, however, do not share this problem. We avoid it by having all workers hold their own flight
                    // though retries. This lets them always access the latest state, even if its been removed from the 
                    // map by a completed leader. This lets us simplify paths so we dont need to start new leaders except
                    // on uninitialized or leader dropped states.

                    let res = pinned.compute(|s| {
                        match s {
                            State::Uninit | State::LeaderDropped => Operation::Set(State::Running),
                            State::Running => Operation::Abort(ComputeEscape::Follow),
                            State::LeaderFailed => Operation::Abort(ComputeEscape::LeaderFailed),
                            State::Success(val) => Operation::Abort(ComputeEscape::Success(val.clone()))
                        }
                    });

                    match res {
                        Compute::Set { .. } => {
                            // If we previously acquired the guard, we drop it here so it decrements the follower count; we're nolonger following.
                            let _ = follower_guard.take();
                            FlightType::Leader(&current_flight)
                        }
                        Compute::Aborted(abort_cause) => {
                            match abort_cause {
                                ComputeEscape::Follow => {
                                    // Increment the follower count if we haven't already done so.
                                    if follower_guard.is_none() {
                                        if !current_flight.try_follow() {
                                            // If we dont have a follower lock and we cant acquire one, we try to get the flight again.
                                            // Once its removed, we can remake the flight and make progress again.
                                            // 
                                            // This whole machinary avoids a situation where a worker was dropped with 0 followers and started 
                                            // removing the flight but another worker gets the flight in between. The worker would now have a
                                            // flight held outside the map, and a new worker could try to work on the same key, see its missing,
                                            // and make a new one, causing 2 instances of work to be active at once.
                                            continue 'acquire;
                                        }
                                        follower_guard = Some(defer(|| current_flight.drop_follower()));
                                    }
                                    // weve now incremented the follower counter properly and can begin following the existing flight.
                                    FlightType::Follower(&current_flight)
                                },
                                ComputeEscape::LeaderFailed => return Err(GroupWorkError::LeaderFailed),
                                ComputeEscape::Success(val) => return Ok(val)
                            }
                        }
                    }
                };

                match flight_type {
                    FlightType::Follower(follower_flight) => {
                        loop {
                            let notified = pin!(follower_flight.wait());

                            match follower_flight.cell_pinned().get() {
                                State::Running | State::Uninit => {}, // pass down to the notified.await while dropping the pinned cell.
                                State::LeaderDropped => continue 'retry,
                                State::LeaderFailed => return Err(GroupWorkError::LeaderFailed),
                                State::Success(val) => return Ok(val.clone()),
                            }

                            notified.await;
                        }
                    }
                    FlightType::Leader(leading_flight) => {
                        let leader = Leader::new(
                            fut.take().expect("Future should be available as leader"),
                            leading_flight
                        );
    
                        // this guard is so that dropped leaders without any followers will remove their entry from the map
                        // if it has followers, a follower can pick up the task.
                        let drop_guard = defer(||  {
                            let acquired_close_guard = leading_flight.close();
                            
                            let _ = self.map.pin().remove_if(key, |_, this| {
                                Arc::ptr_eq(this, leading_flight) && acquired_close_guard
                            });
                        });
                        let result = leader.await;
                        
                        // the leader can always remove it from the map. Old retries will still hold their own existing flight reference.
                        // We also dont need to close the flight to new followers, since they will simply see the success value and clone it out.
                        let _ = self.map.pin().remove_if(key, |_, existing| Arc::ptr_eq(existing, leading_flight)); 
                        drop_guard.cancel(); // we have already removed the entry, no need to try it again.
                        
                        return result.map_err(GroupWorkError::Error)
                    }
                }
            }
        }
    }

    /// Internal helper function for tests.
    #[cfg(test)]
    #[doc(hidden)]
    pub(crate) fn check_if_stale<Q>(&self, key: &Q) -> bool
    where
        Q: Hash + Eq + ?Sized + Send + Sync + ToOwned<Owned = K>,
        K: std::borrow::Borrow<Q>, 
    {
        let map = self.map.pin();
        let res = map.get(key);
        let Some(flight) = res else {
            return false
        };

        let pinned = flight.cell_pinned();
        let state = pinned.get();
        matches!(state, State::Running | State::Uninit)
    }
}