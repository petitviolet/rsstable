use std::{io, collections::{BTreeMap, BTreeSet, HashMap, HashSet}};

pub trait Disktable {
    fn find(&self, key: &String) -> Option<String>;
    fn flush(&mut self, entries: &BTreeMap<String, String>, tombstones: &BTreeSet<String>) -> Result<(), io::Error>;
}
struct DataLayout {
    pub offset: u64,
    pub key_len: usize,
    pub value_len: usize,
}
// for iterator
trait DisktableFetch {
    fn fetch(&self, offset: u64) -> Option<(DataLayout, String, String)>;
}

pub mod default {
    use super::{DataLayout, Disktable, DisktableFetch};
    use io::{BufRead, Read, Write, BufWriter};
    use std::{
        collections::{BTreeMap, BTreeSet},
        fs::{OpenOptions, File},
        io::{self, Seek, SeekFrom},
        path::{PathBuf, Path}, ops::Deref,
    };

    pub struct FileDisktable {
        dir_name: String,
        index: BTreeMap<String, u64>,
    }

    struct RichFile {
      underlying: File,
      dir: String,
      name: String,
    }
    impl RichFile {
        fn open_file(dir_name: impl Into<String>, file_name: impl Into<String>) -> Result<RichFile, io::Error> {
            let dir_name = dir_name.into() ;
            let dir = Path::new(&dir_name);
            let file_name_s: String = file_name.into();
            let path = dir.join(&file_name_s);
            let f = OpenOptions::new()
              .read(true)
              .append(true)
              .create(true)
              .open(&path)?;

            Ok(RichFile {
              underlying: f,
              dir: dir_name,
              name: file_name_s,
            })
        } 
        fn path(&self) -> PathBuf {
            Path::new(&self.dir).join(&self.name)
        }
    }

    impl FileDisktable {
        const IndexDelimiter: &'static str = "\t";
        const DataFileName: &'static str = "data";
        const IndexFileName: &'static str = "index";

        pub fn new(dir_name: String) -> Result<impl Disktable, io::Error> {
          let index_file = RichFile::open_file(&dir_name, Self::IndexFileName)?;
          let index = Self::load_index(&index_file);

            Ok(Self { dir_name, index })
        }
        fn data_file(&self) -> RichFile {
          RichFile::open_file(&self.dir_name, Self::DataFileName).expect("failed to open data file")
        } 
        fn index_file(&self) -> RichFile {
          RichFile::open_file(&self.dir_name, Self::IndexFileName).expect("failed to open index file")
        } 

        fn load_index(index_file: &RichFile) -> BTreeMap<String, u64> {
            let lines = io::BufReader::new(&index_file.underlying).lines();
            lines.fold(BTreeMap::new(), |mut map, line| match line {
                Ok(line) => {
                    let res: Vec<_> = line.split(Self::IndexDelimiter).collect();
                    map.insert(res[0].to_string(), res[1].parse().unwrap());
                    map
                }
                Err(err) => {
                    panic!("failed to load line. err: {:?}", err);
                }
            })
        }

        fn iter<'a>(&'a self) -> DisktableIter<'a> {
            DisktableIter {
                disktable: self,
                next: 0,
            }
        }
    }

    impl DisktableFetch for FileDisktable {
        fn fetch(&self, offset: u64) -> Option<(DataLayout, String, String)> {
            let mut data = self.data_file().underlying;
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
            let mut key_data = Vec::with_capacity(key_len);
            let res = data.read_exact(&mut key_data);
            if res.is_err() {
                return None;
            }

            let value_len = ByteUtils::as_usize(value_len);
            let mut value_data = Vec::with_capacity(value_len);
            let res = data.read_exact(&mut value_data);
            if res.is_err() {
                return None;
            }

            let data_layout = DataLayout {
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
    struct ByteUtils;
    impl ByteUtils {
        fn as_usize(array: [u8; 4]) -> usize {
          u32::from_le_bytes(array) as usize
        }
        fn as_string(array: &[u8]) -> String {
            std::str::from_utf8(array).unwrap().to_string()
        }
    }

    impl Disktable for FileDisktable {
        fn find(&self, key: &String) -> Option<String> {
            self.index
                .get(key)
                .and_then(|offset| match self.fetch(*offset) {
                    Some((_, _key, _value)) if _key == *key => Some(_value),
                    _ => None,
                })
        }

        fn flush(&mut self, entries: &BTreeMap<String, String>, tombstones: &BTreeSet<String>) -> Result<(), io::Error> {
          let mut new_entries = BTreeMap::new();
          self.index.keys().chain(entries.keys()).for_each(|key| { 
            if tombstones.get(key).is_some() {
              return;
            }

            let new_value = entries.get(key).map(|s| s.to_string());
            match self.index.get(key) {
              Some(offset) => {
                match self.fetch(*offset) {
                  Some((_, _, old_value)) => { 
                    let value = new_value.unwrap_or(old_value);
                    new_entries.insert(key, value);
                  },
                  None => { 
                    unreachable!("invalid key({}). offset({})", key, offset);
                  }
                }
              },
              None => {
                let value = new_value.unwrap_or_else(|| panic!("invalid key({})", key));
                new_entries.insert(key, value);
              }
            }
          });

          let new_data_file = RichFile::open_file(&self.dir_name, "tmp_data")?;
          let mut data_writer = BufWriter::new(&new_data_file.underlying);
          let mut offset = 0;

          let mut new_index = BTreeMap::new();
          new_entries.iter().for_each(|(key, value)| { 
            let key_bytes = key.as_bytes();
            let value_bytes = value.as_bytes();
            let written_bytes =
              data_writer.write(&(key_bytes.len() as u32).to_le_bytes()).and_then(|size1| {
                data_writer.write(&(value_bytes.len() as u32).to_le_bytes()).and_then(|size2| {
                  data_writer.write(key_bytes).and_then(|size3| {
                    data_writer.write(value_bytes).and_then(|size4| {
                      data_writer.write(b"\0").map(|size5| {
                        size1 + size2 + size3 + size4 + size5
                      })
                    })
                  })
                })
              }).expect("failed to to write bytes into BufWriter");
            new_index.insert(*key, offset);
            offset += written_bytes;
          });

          let new_index_file = RichFile::open_file(&self.dir_name, "tmp_index")?;
          let mut index_writer = BufWriter::new(&new_index_file.underlying);
          new_index.iter().for_each(|(key, offset)| {
            let line = format!("{}{}{}\n", key, Self::IndexDelimiter, offset);
            index_writer.write(line.as_bytes()).expect(&format!("failed to write a line({})", line));
          });
          std::fs::rename(new_data_file.path(), self.data_file().path())?;
          std::fs::rename(new_index_file.path(), self.index_file().path())?;

          let after = Self::load_index(&self.index_file());
          println!("before: {:?}, after: {:?}", self.index, after);
          self.index = after;
          Ok(())
        }
    }
    struct DisktableIter<'a> {
        disktable: &'a dyn DisktableFetch,
        next: u64,
    }
    impl<'a> Iterator for DisktableIter<'a> {
        type Item = (String, String);
        fn next(&mut self) -> Option<Self::Item> {
            match self.disktable.fetch(self.next) {
                Some((layout, key, value)) => {
                    self.next = self.next + 8 + layout.key_len as u64 + layout.value_len as u64;
                    Some((key, value))
                }
                None => None,
            }
        }
    }
}
