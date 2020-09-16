use super::*;
use crate::sst::rich_file::*;
use byte_utils::*;
use io::{BufWriter, Read, Seek, SeekFrom, Write, BufReader};
use std::mem;

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

impl DataEntry {
  pub unsafe fn as_ref<'a>(&self) -> &'a DataEntry {
    &*self.as_ptr()
  }

  unsafe fn as_ptr(&self) -> *mut DataEntry {
    if mem::size_of::<DataEntry>() == 0 {
        // Just return an arbitrary ZST pointer which is properly aligned
        mem::align_of::<DataEntry>() as *mut DataEntry
    } else {
        self.as_ptr().sub(1)
    }
  }
}

impl DataFile {
    pub const FILE_NAME_PREFIX: &'static str = "data";

    pub fn of(dir_name: &str, data_gen: DataGen) -> DataFile {
        let file = RichFile::open_file(
            dir_name,
            format!("{}_{}", DataFile::FILE_NAME_PREFIX, data_gen),
            FileOption::Append,
        )
        .expect("failed to open data file");

        DataFile { data_gen, file }
    }
    /*
    Data Layout:
    [key length][value length][ key data  ][value data ]\0
    <--4 byte--><--4 byte----><--key_len--><-value_len->
    */
    pub fn read_entry(&self, offset: Offset) -> Option<DataEntry> {
        let mut data = BufReader::new(&self.file.underlying);
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

    pub fn create<'a>(
        &self,
        memtable_entries: &'a MemtableEntries<String, String>,
    ) -> io::Result<BTreeMap<&'a String, Offset>> {
        let MemtableEntries {
            entries,
            tombstones, // TODO: persist records marked as deleted
        } = memtable_entries;

        let new_data_file = RichFile::open_file(&self.file.dir, "tmp_data", FileOption::New)?;
        let mut data_writer = BufWriter::new(&new_data_file.underlying);
        let mut offset: Offset = 0;

        let mut new_index = BTreeMap::new();
        entries.iter().for_each(|(key, value)| {
            let key_bytes = key.as_bytes();
            let value_bytes = value.as_bytes();
            let written_bytes = data_writer
                .write(&ByteUtils::from_usize(key_bytes.len()))
                .and_then(|size1| {
                    data_writer
                        .write(&ByteUtils::from_usize(value_bytes.len()))
                        .and_then(|size2| {
                            data_writer.write(key_bytes).and_then(|size3| {
                                data_writer.write(value_bytes).and_then(|size4| {
                                    data_writer
                                        .write(b"\0")
                                        .map(|size5| size1 + size2 + size3 + size4 + size5)
                                })
                            })
                        })
                })
                .expect("failed to to write bytes into BufWriter");
            new_index.insert(key, offset);
            offset += written_bytes as u64;
        });
        data_writer.flush().expect("failed to write data");
        std::fs::rename(new_data_file.path(), self.file.path())?;
        return Ok(new_index);
    }

    pub fn clear(dir: &str, data_gen: DataGen) -> io::Result<()> {
        let tmp = Self::of(dir, data_gen);
        std::fs::remove_file(tmp.file.path())?;
        Ok(())
    }
}
