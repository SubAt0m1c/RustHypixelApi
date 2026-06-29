
use std::{error::Error as StdError, fmt::Display, io, result::Result as StdResult};

#[derive(Debug)]
pub enum Error {
    IoError(std::io::Error),
    TaskError(String),
    PartitionError(String),
    BucketError(String),
    ExpirationQueueError(String),
    FileNotFoundError(String),
}

impl Error {
    #[inline]
    pub fn partition_str(s: &str) -> Self {
        Self::PartitionError(s.to_owned())
    }

    #[inline]
    pub fn bucket_str(s: &str) -> Self {
        Self::BucketError(s.to_owned())
    }

    #[inline]
    pub fn flume<T>(err: flume::SendError<T>) -> Self {
        Self::ExpirationQueueError(err.to_string())
    }

    #[inline]
    pub fn filenotfound(str: &str) -> Self {
        Self::FileNotFoundError(str.to_owned())
    }
}

impl StdError for Error {}

impl Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::IoError(err) => write!(f, "Io Error: {err}"),
            Self::TaskError(err) => write!(f, "Task Error: {err}"),
            Self::PartitionError(str) => write!(f, "Partition Error: {str}"),
            Self::BucketError(str) => write!(f, "Bucket Error: {str}"),
            Self::ExpirationQueueError(str) => write!(f, "Expiration Queue Error: {str}"),
            Self::FileNotFoundError(str) => write!(f, "File Not Found Error: {str}"),
        }
    }
}

impl From<io::Error> for Error {
    #[inline]
    fn from(value: io::Error) -> Self {
        Self::IoError(value)
    }
}

pub trait ResultExt {
    /// maps a future returning any error into a future returning a database error.
    fn task_err<R, E: StdError>(self) -> impl Future<Output = StdResult<R, Error>>
    where Self: Future<Output = StdResult<R, E>> + Sized
    {
        async move {
            self.await.map_err(|e| Error::TaskError(e.to_string()))
        }
    }
}

impl<F: Future<Output = Result<R, E>>, R, E: StdError> ResultExt for F {}