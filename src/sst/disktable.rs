use std::collections::{HashMap, HashSet};

pub trait Disktable {
    fn find(&self, key: String) -> Option<String>;
    fn flush(&mut self, tombstones: HashSet<String>, entries: HashMap<String, String>);
}
struct DataLayout {
    pub offset: u64,
    pub key_len: usize,
    pub value_len: usize,
}
trait DisktableFetch {
    fn fetch(&self, offset: u64) -> Option<(DataLayout, String, String)>;
}

pub mod default {
    use super::{DataLayout, Disktable, DisktableFetch};
    use io::{BufRead, Read};
    use std::{
        collections::{BTreeMap, HashMap, HashSet},
        fs::File,
        io::{self, Seek, SeekFrom},
        ops::Deref,
        path::Path,
    };

    pub struct FileDisktable {
        dir_name: String,
        index: BTreeMap<String, u64>,
    }

    impl FileDisktable {
        const IndexDelimiter: &'static str = "\t";
        const DataFileName: &'static str = "data";
        const IndexFileName: &'static str = "index";

        pub fn new(dir_name: String) -> Result<impl Disktable, io::Error> {
            let index = Self::load_index(&dir_name)?;

            Ok(Self {
                dir_name,
                index,
            })
        }
        fn prepare_file(path: &Path) -> Result<File, io::Error> {
            File::open(path).or_else(|err| File::create(path))
        }

        fn data_file(dir_name: &String) -> Result<File, io::Error> {
            let dir = Path::new(dir_name);
            Self::prepare_file(dir.join(Self::DataFileName).as_path())
        }

        fn load_index(dir_name: &String) -> Result<BTreeMap<String, u64>, io::Error> {
            let dir = Path::new(dir_name);
            let index_dir =dir.join(Self::IndexFileName);
            let index_path = index_dir.as_path();
            Self::prepare_file(index_path)
                .map(|file| io::BufReader::new(file).lines())
                .map(|lines| {
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
          let mut data = Self::data_file(&self.dir_name).unwrap();
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

            let key_len = ByteUtils::as_usize(&key_len);
            let mut key_data = Vec::with_capacity(key_len);
            let res = data.read_exact(&mut key_data);
            if res.is_err() {
                return None;
            }

            let value_len = ByteUtils::as_usize(&value_len);
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
        fn as_usize(array: &[u8; 4]) -> usize {
            let result = ((array[0] as u32) << 24)
                + ((array[1] as u32) << 16)
                + ((array[2] as u32) << 8)
                + ((array[3] as u32) << 0);
            result as usize
        }
        fn as_string(array: &[u8]) -> String {
            std::str::from_utf8(array).unwrap().to_string()
        }
    }

    impl Disktable for FileDisktable {
        fn find(&self, key: String) -> Option<String> {
            self.index
                .get(&key)
                .and_then(|offset| match self.fetch(*offset) {
                    Some((_, _key, _value)) if _key == key => Some(_value),
                    _ => None,
                })
        }

        fn flush(&mut self, tombstones: HashSet<String>, entries: HashMap<String, String>) {
            todo!()
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
