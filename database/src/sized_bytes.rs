use bytes::Buf;

pub struct SizedBytes<const N: usize> {
    inner: [u8; N],
    cursor: usize
}

impl<const N: usize> Buf for SizedBytes<N> {
    #[inline]
    fn remaining(&self) -> usize {
        self.inner.len() - self.cursor
    }
    
    #[inline]
    fn chunk(&self) -> &[u8] {
        &self.inner[self.cursor..]
    }
    
    #[inline]
    fn advance(&mut self, cnt: usize) {
        self.cursor += cnt
    }
}

impl<const N: usize> From<[u8; N]> for SizedBytes<N> {
    #[inline]
    fn from(value: [u8; N]) -> Self {
        Self {
            inner: value,
            cursor: 0,
        }
    }
}