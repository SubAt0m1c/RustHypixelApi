use std::{error::Error, fmt::{Debug, Display}};

#[derive(Debug, PartialEq, Eq)]
pub enum GroupWorkError<E> {
    LeaderFailed,
    Error(E)
}

impl<E> Display for GroupWorkError<E> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::LeaderFailed => Display::fmt("Leader returned an error!", f),
            Self::Error(_) => Display::fmt("Leader returned an error", f),
        }
    }
}

impl<E: Debug> Error for GroupWorkError<E> {}