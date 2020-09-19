use super::*;
use crate::sst::rich_file::*;
use byte_utils::*;
use io::{BufWriter, Read, Seek, SeekFrom, Write};

pub(crate) struct DataFile {
    pub data_gen: DataGen,
    pub file: RichFile,
}
pub(crate) struct DataEntry {
    pub data_gen: DataGen,
    pub offset: Offset,
    pub size: usize,
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
        let mut data = &self.file.underlying;
        data.seek(SeekFrom::Start(offset)).unwrap();
        let mut size: [u8; 4] = [0; 4];
        let res = data.read_exact(&mut size);
        if res.is_err() {
            return None;
        }
        let size = ByteUtils::as_usize(&size);
        let mut bytes = vec![0u8; size];
        let res = data.read_exact(&mut bytes);
        if res.is_err() {
            return None;
        }
        let key_len = ByteUtils::as_usize(bytes.get(4..8).unwrap());
        let value_len = ByteUtils::as_usize(bytes.get(8..12).unwrap());
        let key_data = bytes.get(12..(12 + key_len)).unwrap();
        let value_data = bytes.get((12 + key_len)..(12 + key_len + value_len)).unwrap();
        Some(DataEntry {
            data_gen: self.data_gen,
            offset,
            size,
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
            let size = 4 + key_bytes.len() + 4 + value_bytes.len();
            let bytes: Vec<u8> =[
              &ByteUtils::from_usize(size), 
              &ByteUtils::from_usize(key_bytes.len()),
              &ByteUtils::from_usize(value_bytes.len()),
              key_bytes,
              value_bytes,
              b"\0"
            ].concat();
            data_writer.write(&bytes)
                .expect("failed to to write bytes into BufWriter");
            new_index.insert(key, offset);
            offset += (size + 1) as u64;
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
