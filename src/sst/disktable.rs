mod rich_file;
mod byte_utils;
use std::{
    collections::{BTreeMap, BTreeSet},
    io,
};

pub trait Disktable {
    fn find(&self, key: &String) -> Option<String>;
    fn flush(
        &mut self,
        entries: Box<BTreeMap<String, String>>,
        tombstones: Box<BTreeSet<String>>,
    ) -> Result<(), io::Error>;
    fn clear(&mut self) -> Result<(), io::Error>;
}
type DataGen = i32; // data generation
type Offset = u64;

struct DataLayout {
    pub data_gen: DataGen,
    pub offset: Offset,
    pub key_len: usize,
    pub value_len: usize,
}
pub mod default {
    use super::{DataLayout, Disktable, DataGen, Offset, rich_file::{FileOption, RichFile}, byte_utils::ByteUtils};
    use io::{BufRead, BufReader, BufWriter, Read, Write};
    use regex::Regex;
    use std::{
        collections::{BTreeMap, BTreeSet},
        fs::{File, OpenOptions},
        io::{self, Seek, SeekFrom},
        ops::Deref,
        path::{Path, PathBuf},
    };

    pub struct FileDisktable {
        dir_name: String,
        data_gen: DataGen,
        flushing: Option<(Box<BTreeMap<String, String>>, Box<BTreeSet<String>>)>,
    }

    impl FileDisktable {
        const INDEX_DELIMITER: &'static str = "\t";
        const DATA_FILE_NAME_PREFIX: &'static str = "data";
        const INDEX_FILE_NAME: &'static str = "index";

        pub fn new(dir_name: String) -> Result<impl Disktable, io::Error> {
            std::fs::create_dir_all(&dir_name).expect("failed to create directory");
            let data_gen = Self::get_latest_data_gen(&dir_name)?;
            let flushing = None;

            Ok(Self {
                data_gen,
                dir_name,
                flushing,
            })
        }

        fn get_data_gens(dir_name: &String) -> io::Result<Vec<DataGen>> {
            std::fs::read_dir(dir_name).map(|dir| {
                let mut list = dir.fold(vec![], |mut acc, entry| {
                    let file_name = entry.unwrap().file_name();
                    let file_name = file_name.to_string_lossy();
                    match Regex::new(&format!("{}_(?P<gen>\\d+)", Self::DATA_FILE_NAME_PREFIX))
                        .unwrap()
                        .captures(&file_name)
                    {
                        Some(cap) => {
                            acc.push(cap["gen"].parse::<DataGen>().unwrap());
                            acc
                        }
                        None => acc,
                    }
                });
                list.sort();
                list
            })
        }

        fn get_latest_data_gen(dir_name: &String) -> io::Result<DataGen> {
            Self::get_data_gens(dir_name).map(|list| *list.last().unwrap_or(&0))
        }

        fn data_file(&self, gen: DataGen) -> RichFile {
            RichFile::open_file(
                &self.dir_name,
                format!("{}_{}", Self::DATA_FILE_NAME_PREFIX, gen),
                FileOption::Append,
            )
            .expect("failed to open data file")
        }
        fn index_file(&self, data_gen: DataGen) -> RichFile {
            RichFile::open_file(&self.dir_name, format!("{}_{}", Self::INDEX_FILE_NAME, data_gen), FileOption::Append)
                .expect("failed to open index file")
        }

        fn load_index(index_file: &RichFile) -> BTreeMap<String, (DataGen, Offset)> {
            let lines = io::BufReader::new(&index_file.underlying).lines();
            lines.fold(BTreeMap::new(), |mut map, line| match line {
                Ok(line) => {
                    let res: Vec<_> = line.split(Self::INDEX_DELIMITER).collect();
                    map.insert(
                        res[0].to_string(),
                        (res[1].parse().unwrap(), res[2].parse().unwrap()),
                    );
                    map
                }
                Err(err) => {
                    panic!("failed to load line. err: {:?}", err);
                }
            })
        }

        fn find_index(&self, key: &String) -> io::Result<Option<(DataGen, Offset)>> { 
          Self::get_data_gens(&self.dir_name)
            .map(|gens| {
              gens.iter().find_map(|gen: &DataGen| {
                io::BufReader::new(&self.index_file(*gen).underlying).lines().find_map(|line| { 
                  match line {
                    Ok(line) => {
                        let res: Vec<_> = line.split(Self::INDEX_DELIMITER).collect();
                        if key == res[0] {
                          Some((*gen, res[1].parse().unwrap()))
                        } else { None }
                    }
                    Err(err) => {
                        panic!("failed to load line. err: {:?}", err);
                    }
                  }
                })
              })
            })
        }


        fn fetch(&self, data_gen: DataGen, offset: Offset) -> Option<(DataLayout, String, String)> {
            let mut data = self.data_file(data_gen).underlying;
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

            let data_layout = DataLayout {
                data_gen,
                offset,
                key_len,
                value_len,
            };
            Some((
                data_layout,
                ByteUtils::as_string(&key_data),
                ByteUtils::as_string(&value_data),
            ))
        }
    } 

    impl Disktable for FileDisktable {
        fn find(&self, key: &String) -> Option<String> {
            self.flushing
                .as_ref()
                .and_then(|(entries, tombstones)| {
                    if tombstones.get(key).is_none() {
                        entries.get(key).map(|s| s.to_string())
                    } else {
                        None
                    }
                })
                .or_else(|| {
                    self.find_index(key).unwrap()
                    .and_then(|(data_gen, offset)| {
                        match self.fetch(data_gen, offset) {
                            Some((_, _key, _value)) if _key == *key => Some(_value),
                            _ => None,
                        }
                    })
                })
        }

        fn flush(
            &mut self,
            entries: Box<BTreeMap<String, String>>,
            tombstones: Box<BTreeSet<String>>,
        ) -> Result<(), io::Error> {
            self.flushing = Some((entries, tombstones));
            let (entries, tombstones) = self.flushing.as_ref().unwrap();

            let next_data_gen = self.data_gen + 1;
            let new_data_file = RichFile::open_file(&self.dir_name, "tmp_data", FileOption::New)?;
            let mut data_writer = BufWriter::new(&new_data_file.underlying);
            let mut offset = 0;

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
                offset += written_bytes;
            });
            data_writer.flush().expect("failed to write data");

            let new_index_file = RichFile::open_file(&self.dir_name, "tmp_index", FileOption::New)?;
            let mut index_writer = BufWriter::new(&new_index_file.underlying);
            new_index.iter().for_each(|(key, offset)| {
                let line = format!(
                    "{}{}{}\n",
                    key, Self::INDEX_DELIMITER, offset
                );
                index_writer
                    .write(line.as_bytes())
                    .expect(&format!("failed to write a line({})", line));
            });
            index_writer.flush().expect("failed to write index data");
            std::fs::rename(new_data_file.path(), self.data_file(next_data_gen).path())?;
            std::fs::rename(new_index_file.path(), self.index_file(next_data_gen).path())?;

            println!("entries: {:?}", entries);
            self.data_gen = next_data_gen;
            self.flushing = None;
            Ok(())
        }

        fn clear(&mut self) -> Result<(), io::Error> {
            (0..=self.data_gen).for_each(|gen| {
                std::fs::remove_file(self.data_file(gen).path()).expect("failed to remove file");
                std::fs::remove_file(self.index_file(gen).path()).expect("failed to remove index file");
            });
            self.data_gen = 0;
            Ok(())
        }
    }
}
