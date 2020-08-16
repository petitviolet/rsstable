use super::*;
use byte_utils::*;
use io::{Read, Seek, SeekFrom};
use rich_file::*;

pub struct DataFile(RichFile);
pub struct DataEntry {
    pub data_gen: DataGen,
    pub offset: Offset,
    pub key_len: usize,
    pub value_len: usize,
    pub key: String,
    pub value: String,
}

impl DataFile {
    pub fn of(rich_file: RichFile) -> DataFile {
        DataFile(rich_file)
    }
    /*
    Data Layout:
    [key length][value length][ key data  ][value data ]\0
    <--4 byte--><--4 byte----><--key_len--><-value_len->
    */
    pub fn read_entry(&self, data_gen: DataGen, offset: Offset) -> Option<DataEntry> {
        let mut data = &self.0.underlying;
        data.seek(SeekFrom::Start(offset)).unwrap();
        let mut key_len: [u8; 4] = [0; 4];
        let res = data.read_exact(&mut key_len);
        if res.is_err() {
            return None;
        }

        let mut value_len: [u8; 4] = [0; 4];
        let res = data.read_exact(&mut value_len);
        if res.is_err() {
            return None;
        }

        let key_len = ByteUtils::as_usize(key_len);
        if key_len == 0 {
            return None;
        }
        let mut key_data = vec![0u8; key_len];
        let res = data.read_exact(&mut key_data);
        if res.is_err() {
            return None;
        } else if key_data.len() != key_len {
            panic!(
                "invalid key. offset: {}, key_len: {}, key_data: {:?}",
                offset, key_len, key_data
            );
        }

        let value_len = ByteUtils::as_usize(value_len);
        if value_len == 0 {
            return None;
        }
        // let mut value_data = Vec::with_capacity(value_len); // doesn't work somehow
        let mut value_data = vec![0u8; value_len];
        let res = data.read_exact(&mut value_data);
        if res.is_err() {
            return None;
        } else if value_data.len() != value_len {
            panic!(
                "invalid value. offset: {}, lvalue_len: {}, value_data: {:?}",
                offset, value_len, value_data
            );
        }

        Some(DataEntry {
            data_gen,
            offset,
            key_len,
            value_len,
            key: ByteUtils::as_string(&key_data),
            value: ByteUtils::as_string(&value_data),
        })
    }
}
