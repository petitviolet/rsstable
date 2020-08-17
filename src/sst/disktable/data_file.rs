use super::*;
use byte_utils::*;
use io::{Read, Seek, SeekFrom};
use rich_file::*;

pub(crate) struct DataFile {
  pub data_gen: DataGen,
  pub file: RichFile,
}
pub(crate) struct DataEntry {
    pub data_gen: DataGen,
    pub offset: Offset,
    pub key_len: usize,
    pub value_len: usize,
    pub key: String,
    pub value: String,
}

impl DataFile {
    pub const FILE_NAME_PREFIX: &'static str = "data";

    pub fn of(dir_name: &str, data_gen: DataGen) -> DataFile {
        let file = RichFile::open_file(
            dir_name,
            format!("{}_{}", DataFile::FILE_NAME_PREFIX, data_gen),
            FileOption::Append,
        ).expect("failed to open data file");

        DataFile {
          data_gen,
          file,
        }
    }
    /*
    Data Layout:
    [key length][value length][ key data  ][value data ]\0
    <--4 byte--><--4 byte----><--key_len--><-value_len->
    */
    pub fn read_entry(&self, offset: Offset) -> Option<DataEntry> {
        let mut data = &self.file.underlying;
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
            data_gen: self.data_gen,
            offset,
            key_len,
            value_len,
            key: ByteUtils::as_string(&key_data),
            value: ByteUtils::as_string(&value_data),
        })
    }

    pub fn clear(dir: &str, data_gen: DataGen) -> io::Result<()> {
        let tmp = Self::of(dir, data_gen);
        std::fs::remove_file(tmp.file.path())?;
        Ok(())
    }
}
