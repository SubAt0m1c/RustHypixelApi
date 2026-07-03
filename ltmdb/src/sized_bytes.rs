use std::{hash, ops::Deref};

use bytes::{Buf, Bytes};

const MAX_INLINE_WORD_LENGTH: usize = size_of::<Bytes>() - size_of::<usize>() - size_of::<u8>() - size_of::<u8>();

#[derive(Clone, Debug)]
pub struct SizedBytes {
    inner: Inner
}

#[derive(Clone, Debug)]
enum Inner {
    Inline {
        bytes: [u8; MAX_INLINE_WORD_LENGTH],
        len: u8,
        cursor: u8,
    },
    Bytes(Bytes),
}

impl Inner {
    #[inline]
    fn as_slice(&self) -> &[u8] {
        match self {
            Self::Inline { bytes, len, cursor } => &bytes[*cursor as usize..*len as usize],
            Self::Bytes(b) => b.chunk()
        }
    }
}

impl hash::Hash for SizedBytes {
    fn hash<H: hash::Hasher>(&self, state: &mut H) {
        self.as_slice().hash(state);
    }
}

impl PartialEq for SizedBytes {
    fn eq(&self, other: &Self) -> bool {
        self.as_slice().eq(other.as_slice())
    }
}
impl Eq for SizedBytes {}

impl PartialOrd for SizedBytes {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        self.as_slice().partial_cmp(other.as_slice())
    }
}

impl Ord for SizedBytes {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.as_slice().cmp(other.as_slice())
    }
}

impl SizedBytes {
    #[inline]
    fn as_slice(&self) -> &[u8] {
        self.inner.as_slice()
    }
}

impl Deref for SizedBytes {
    type Target = [u8];

    #[inline]
    fn deref(&self) -> &Self::Target {
        self.as_slice()
    }
}

impl Buf for SizedBytes {
    #[inline]
    fn remaining(&self) -> usize {
        self.len()
    }
    #[inline]
    fn chunk(&self) -> &[u8] {
        self.as_slice()
    }
    #[inline]
    fn advance(&mut self, cnt: usize) {
        match &mut self.inner {
            Inner::Inline { len, cursor, .. } => {
                assert!(cnt <= (*len - *cursor) as usize, "cnt {:?} must be less than remaining {:?}", cnt, *len - *cursor);
                *cursor += cnt as u8
            }
            Inner::Bytes(b) => b.advance(cnt),
        }
    }
    #[inline]
    fn copy_to_bytes(&mut self, len: usize) -> Bytes {
        match &mut self.inner {
            Inner::Bytes(b) => b.copy_to_bytes(len),
            Inner::Inline { .. } => {
                let b = Bytes::copy_from_slice(&self.chunk()[..len]);
                self.advance(len);
                b
            }
        }
    }
}

impl From<&[u8]> for SizedBytes {
    #[inline]
    fn from(value: &[u8]) -> Self {
        if value.len() <= MAX_INLINE_WORD_LENGTH {
            let mut bytes = [0u8; MAX_INLINE_WORD_LENGTH];
            bytes[..value.len()].copy_from_slice(value);
            return SizedBytes { inner: Inner::Inline { bytes, len: value.len() as u8, cursor: 0 } }
        }
        SizedBytes { inner: Inner::Bytes(Bytes::copy_from_slice(value)) }
    }
}

impl<const N: usize> From<[u8; N]> for SizedBytes {
    #[inline]
    fn from(value: [u8; N]) -> Self {
        if const { N <= MAX_INLINE_WORD_LENGTH } {
            let mut bytes = [0u8; MAX_INLINE_WORD_LENGTH];
            bytes[..N].copy_from_slice(&value);
            return SizedBytes { inner: Inner::Inline { bytes, len: value.len() as u8, cursor: 0 } }
        }
        SizedBytes { inner: Inner::Bytes(Bytes::copy_from_slice(&value)) }
    }
}

impl From<Bytes> for SizedBytes {
    #[inline]    
    fn from(value: Bytes) -> Self {
        SizedBytes { inner: Inner::Bytes(value) }
    }
}