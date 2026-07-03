use crate::error::Error;

pub(crate) trait SendRuntime: Runtime + Send + Sync + 'static {}
impl<T: Runtime + Send + Sync + 'static> SendRuntime for T {}

pub trait Runtime {
    /// Spawns a runtime scheduled task.
    /// 
    /// Currently only used to handle the expiration queue.
    fn spawn<F>(task: F)
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static;

    /// Spawns a runtime scheduled blocking task
    /// 
    /// This is used to make blocking file io async.
    fn spawn_blocking<T, R>(task: T) -> impl Future<Output = Result<R, Error>> + Send
    where
        T: FnOnce() -> R + Send + 'static,
        R: Send + 'static;
}