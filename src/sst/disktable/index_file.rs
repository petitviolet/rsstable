use super::*;
use byte_utils::*;
use io::{BufRead, BufWriter, Read, Seek, SeekFrom, Write};
use rich_file::*;
use std::{fmt::Debug, io::BufReader};

pub(crate) struct IndexFile {
    data_gen: DataGen,
    file: RichFile,
    skip_index_file: RichFile,
}

impl Debug for IndexFile {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "IndexFile {{ data_gen: {}, file: {:?}, skip_index_file: {:?} }}",
            self.data_gen,
            self.file.path(),
            self.skip_index_file.path()
        )
    }
}
pub(crate) struct IndexEntry {
    pub key: String,
    pub data_gen: DataGen,
    pub offset: Offset,
}

impl IndexFile {
    const INDEX_DELIMITER: &'static str = "\t";
    const INDEX_FILE_NAME: &'static str = "index";

    pub fn of(data_gen: DataGen, dir: &str) -> IndexFile {
        IndexFile {
            data_gen,
            file: Self::index_file(dir, &data_gen),
            skip_index_file: Self::skip_index_file(dir, &data_gen),
        }
    }
    fn index_file(dir_name: &str, data_gen: &DataGen) -> RichFile {
        RichFile::open_file(
            dir_name,
            format!("{}_{}", Self::INDEX_FILE_NAME, data_gen),
            FileOption::Append,
        )
        .expect("failed to open index file")
    }

    fn skip_index_file(dir_name: &str, data_gen: &DataGen) -> RichFile {
        RichFile::open_file(
            dir_name,
            format!("{}_{}_skip", Self::INDEX_FILE_NAME, data_gen),
            FileOption::Append,
        )
        .expect("failed to open skip index file")
    }

    /* index file layout
    [key len][key][offset in data file]\0...
    */
    pub fn find_index(&self, key: &str) -> Option<IndexEntry> {
        let start_offset = self.find_index_seek_from(key);
        let mut index = &self.file.underlying;
        index.seek(SeekFrom::Start(start_offset)).unwrap();
        loop {
            let mut key_len: [u8; 4] = [0; 4];
            let res = index.read_exact(&mut key_len);
            if res.is_err() {
                // println!("failed to read. err: {:?}", res);
                return None;
            }

            let key_len = ByteUtils::as_usize(key_len);
            if key_len == 0 {
                return None;
            }
            let mut key_data = vec![0u8; key_len];
            let res = index.read_exact(&mut key_data);
            if res.is_err() {
                // println!("failed to read. err: {:?}", res);
                return None;
            } else if key_data.len() != key_len {
                panic!(
                    "invalid key. key_len: {}, key_data: {:?}",
                    key_len, key_data
                );
            }
            let _key = ByteUtils::as_string(&key_data);
            if _key != *key {
                index.read_exact(&mut [0; 9]); // offset + \0
                continue;
            }

            let mut offset: [u8; 8] = [0; 8];
            let res = index.read_exact(&mut offset);
            if res.is_err() {
                return None;
            }
            let offset = ByteUtils::as_u64(offset);
            return Some(IndexEntry {
                key: _key,
                data_gen: self.data_gen,
                offset,
            });
        }
        println!("cannot find index for key({})", key);
        None
    }

    /* skip file layout:
    [key 0]\t[offset in this file]
    [key N]\t[offset in this file]
    [key2N]\t[offset in this file]
    */
    fn find_index_seek_from(&self, key: &str) -> Offset {
        let mut lines = BufReader::new(&self.skip_index_file.underlying).lines();
        let mut last_offset = 0;
        lines
            .find_map(|line| {
                let line = line.expect("failed to read a line");

                let res: Vec<_> = line.split(Self::INDEX_DELIMITER).collect();
                let _key = res[0].to_string();
                let offset = res[1].parse::<Offset>().unwrap();
                if key <= _key.as_ref() {
                    Some(offset)
                } else {
                    last_offset = offset;
                    None
                }
            })
            .unwrap_or(last_offset)
    }

    pub fn create_index(&self, index_entries: &BTreeMap<&String, Offset>) -> io::Result<()> {
        let dir_name = &self.file.dir;
        let new_index_file = RichFile::open_file(
            dir_name,
            format!("tmp_index_{}", self.data_gen),
            FileOption::New,
        )?;
        let mut index_writer = BufWriter::new(&new_index_file.underlying);

        let new_skip_index_file = RichFile::open_file(dir_name, "tmp_skip_index", FileOption::New)?;
        let mut skip_index_writer = BufWriter::new(&new_skip_index_file.underlying);
        let mut index_offset = 0;
        let num = index_entries.len();
        let skip_index_num = 30;
        (0..num)
            .zip(index_entries.iter())
            .for_each(|(idx, (key, offset))| {
                let key_bytes = key.as_bytes();
                let written_bytes = index_writer
                    .write(&ByteUtils::from_usize(key_bytes.len()))
                    .and_then(|size1| {
                        index_writer.write(key_bytes).and_then(|size2| {
                            index_writer
                                .write(&ByteUtils::from_u64(*offset))
                                .and_then(|size3| {
                                    index_writer
                                        .write(b"\0")
                                        .map(|size4| size1 + size2 + size3 + size4)
                                })
                        })
                    })
                    .expect("failed to to write bytes into BufWriter");

                if idx % skip_index_num == skip_index_num - 1 {
                    skip_index_writer
                        .write(
                            format!("{}{}{}\n", key, Self::INDEX_DELIMITER, index_offset)
                                .as_bytes(),
                        )
                        .expect("failed to to write bytes into BufWriter");
                }

                index_offset += written_bytes;
            });
        index_writer.flush()?;
        skip_index_writer.flush()?;

        std::fs::rename(new_index_file.path(), self.file.path())?;
        std::fs::rename(new_skip_index_file.path(), self.skip_index_file.path())?;
        Ok(())
    }

    pub fn clear(data_gen: DataGen, dir: &str) -> io::Result<()> {
        let tmp = Self::of(data_gen, dir);
        std::fs::remove_file(tmp.file.path())?;
        std::fs::remove_file(tmp.skip_index_file.path())?;
        Ok(())
    }
}
