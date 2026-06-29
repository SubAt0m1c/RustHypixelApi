use crate::error::Error;

pub(crate) trait SendRuntime: Runtime + Send + Sync + 'static {}
impl<T: Runtime + Send + Sync + 'static> SendRuntime for T {}

pub trait Runtime {
    fn spawn<F>(task: F)
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static;

    fn spawn_blocking<T, R>(task: T) -> impl Future<Output = Result<R, Error>> + Send
    where
        T: FnOnce() -> R + Send + 'static,
        R: Send + 'static;
}