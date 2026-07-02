
use std::{error::Error as StdError, fmt::Display, io, result::Result as StdResult};

#[derive(Debug)]
pub struct Error {
    kind: ErrorKind,
    error: ErrorContent,
}

#[derive(Debug)]
enum ErrorContent {
    Simple(&'static str),
    Err(Box<dyn StdError + Send + Sync>)
}

#[derive(Debug)]
pub enum ErrorKind {
    IoError,
    TaskError,
    PartitionError,
    BucketError,
    QueueError,
    Other(&'static str)
}

impl Display for ErrorKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Other(str) => write!(f, "{}", str),
            _ => write!(f, "{:?}", self),
        }
    }
}

impl Error {
    pub const BUCKET_NOT_FOUND: Self = Self { kind: ErrorKind::BucketError, error: ErrorContent::Simple("Bucket Not Found!") };
    pub const PARTITION_NOT_FOUND: Self = Self { kind: ErrorKind::PartitionError, error: ErrorContent::Simple("Partition Not Found!") };
    
    pub fn new(kind: ErrorKind, err: impl StdError + Send + Sync + 'static) -> Self {
        Self {
            kind, error: ErrorContent::Err(Box::new(err))
        }
    }

    pub fn simple(kind: ErrorKind, err: &'static str) -> Self {
        Self {
            kind, error: ErrorContent::Simple(err)
        }
    }

    pub fn io(err: io::Error) -> Self {
        Self::new(ErrorKind::IoError, err)
    }

    pub fn queue(err: impl StdError + Send + Sync + 'static) -> Self {
        Self::new(ErrorKind::QueueError, err)
    }
}

impl StdError for Error {}

impl Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}: ", self.kind)?;
        match &self.error {
            ErrorContent::Err(e) => e.fmt(f),
            ErrorContent::Simple(str) => str.fmt(f)
        }
    }
}

impl From<io::Error> for Error {
    #[inline]
    fn from(value: io::Error) -> Self {
        Self::new(ErrorKind::IoError, value)
    }
}

pub trait ResultExt {
    /// maps a future returning any error into a future returning a database error.
    fn task_err<R, E: StdError + Send + Sync + 'static>(self) -> impl Future<Output = StdResult<R, Error>>
    where Self: Future<Output = StdResult<R, E>> + Sized
    {
        async move {
            self.await.map_err(|e| Error::new(ErrorKind::TaskError, e))
        }
    }

    fn flatten<R, E: Into<Error> + Send + Sync + 'static>(self) -> impl Future<Output = StdResult<R, Error>>
    where Self: Future<Output = StdResult<StdResult<R, Error>, E>> + Sized
    {
        async move {
            self.await.map_err(Into::into).flatten()
        }
    }
}

impl<F: Future<Output = Result<R, E>>, R, E: StdError> ResultExt for F {}