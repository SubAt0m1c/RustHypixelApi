use lz4_flex::{block::DecompressError, compress_prepend_size, decompress_size_prepended};

pub fn compress(data: &[u8]) -> Vec<u8> {
    compress_prepend_size(data)
}

/// expected input should be gathered from `compress()`
pub fn decompress(data: &[u8]) -> Result<Vec<u8>, DecompressError> {
    decompress_size_prepended(data)
}
