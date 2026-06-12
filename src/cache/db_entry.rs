use actix_web::{cookie::time::UtcDateTime, web::{Buf, BufMut, Bytes, BytesMut}};

pub struct DbEntry(Bytes);

impl DbEntry {
    pub fn construct(data: &[u8], added: UtcDateTime) -> Self {
        let mut bytes = BytesMut::with_capacity(data.len() + size_of::<i64>());
        bytes.put_i64(added.unix_timestamp());
        bytes.put_slice(data);
        Self(bytes.freeze())
    }

    pub fn deconstruct_slice(data: &[u8]) -> (UtcDateTime, &[u8]) {
        let mut data = data;
        let time = UtcDateTime::from_unix_timestamp(data.get_i64()).expect("DbEntry should only be constructed with a valid utc time.");
        (time, data)
    }
    
    // pub fn deconstruct(&self) -> (UtcDateTime, Bytes) {
    //     let mut bytes = self.0.clone();
    //     let time = UtcDateTime::from_unix_timestamp(bytes.get_i64()).expect("DbEntry should only be constructed with a valid utc time.");
    //     (time, bytes)
    // }

    pub fn data(&self) -> Bytes {
        self.0.slice(size_of::<i64>()..)
    }

    pub fn bytes(&self) -> Bytes {
        self.0.clone()
    }
}