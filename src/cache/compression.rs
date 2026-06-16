use lz4_flex::{block::DecompressError, compress_prepend_size, decompress_size_prepended};

pub fn compress_data(data: &[u8]) -> Vec<u8> {
    compress_prepend_size(data)
}

pub fn extract_data(data: &[u8]) -> Result<Vec<u8>, DecompressError> {
    decompress_size_prepended(data)
}
