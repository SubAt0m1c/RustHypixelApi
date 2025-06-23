use lz4::{Decoder, EncoderBuilder};
use std::io;
use std::io::{Read, Write};

pub fn compress_data(data: &[u8]) -> Result<Vec<u8>, io::Error> {
    let mut encoder = EncoderBuilder::new().build(Vec::new())?;
    encoder.write_all(data)?;
    let (compressed, result) = encoder.finish();
    result?;
    Ok(compressed)
}

pub fn extract_data(data: &[u8]) -> Result<Vec<u8>, io::Error> {
    let mut decoder = Decoder::new(data)?;
    let mut result = Vec::new();
    decoder.read_to_end(&mut result)?;
    Ok(result)
}
