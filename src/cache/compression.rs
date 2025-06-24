use lz4_flex::{compress_prepend_size, decompress_size_prepended};
use std::io;

pub fn compress_data(data: &[u8]) -> Vec<u8> {
    compress_prepend_size(data)
}

pub fn extract_data(data: &[u8]) -> Result<Vec<u8>, io::Error> {
    decompress_size_prepended(data).map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))
}
