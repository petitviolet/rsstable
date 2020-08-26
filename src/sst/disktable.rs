mod byte_utils;
mod data_file;
mod index_file;

use super::memtable::MemtableEntries;
use std::{collections::BTreeMap, io};

pub(crate) trait Disktable {
    fn find(&self, key: &str) -> Option<String>;
    fn flush(&mut self, memtable_entries: MemtableEntries<String, String>)
        -> Result<(), io::Error>;
    fn clear(&mut self) -> Result<(), io::Error>;
}
type DataGen = i32; // data generation
type Offset = u64;

pub(crate) mod default {
    use super::{byte_utils::*, data_file::*, index_file::*, *};
    use crate::sst::memtable::{self, MemtableEntries};
    use io::{BufWriter, Write};
    use regex::Regex;
    use std::{collections::BTreeMap, io};

    pub(crate) struct FileDisktable {
        dir_name: String,
        data_gen: DataGen,
        flushing: Option<MemtableEntries<String, String>>,
    }

    impl FileDisktable {
        pub fn new(dir_name: &str) -> Result<impl Disktable, io::Error> {
            std::fs::create_dir_all(&dir_name).expect("failed to create directory");
            let data_gen = Self::get_latest_data_gen(dir_name)?;
            let flushing = None;

            Ok(Self {
                data_gen,
                dir_name: dir_name.to_string(),
                flushing,
            })
        }

        fn get_data_gens(dir_name: &str) -> io::Result<Vec<DataGen>> {
            std::fs::read_dir(dir_name).map(|dir| {
                let mut list = dir.fold(vec![], |mut acc, entry| {
                    let file_name = entry.unwrap().file_name();
                    let file_name = file_name.to_string_lossy();
                    match Regex::new(&format!("{}_(?P<gen>\\d+)", DataFile::FILE_NAME_PREFIX))
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

        fn get_latest_data_gen(dir_name: &str) -> io::Result<DataGen> {
            Self::get_data_gens(dir_name).map(|list| *list.last().unwrap_or(&0))
        }

        fn data_file(&self, gen: DataGen) -> DataFile {
            DataFile::of(&self.dir_name, gen)
        }

        fn index_file(&self, data_gen: DataGen) -> IndexFile {
            IndexFile::of(data_gen, &self.dir_name)
        }

        fn fetch(&self, data_gen: DataGen, offset: Offset) -> Option<(String, String)> {
            let entry = self.data_file(data_gen).read_entry(offset);
            entry.map(|entry| (entry.key, entry.value))
        }
    }

    impl Disktable for FileDisktable {
        fn find(&self, key: &str) -> Option<String> {
            let find_from_disk = || {
                (0..=self.data_gen).rev().find_map(|data_gen| {
                    self.index_file(data_gen)
                        .find_index(key)
                        .and_then(|index_entry| {
                            self.fetch(index_entry.data_gen, index_entry.offset)
                                .filter(|(_key, _)| _key == key)
                                .map(|(_, value)| value)
                        })
                })
            };
            match self.flushing.as_ref() {
                Some(mem_entries) => match mem_entries.get(&key.to_string()) {
                    memtable::GetResult::Found(value) => Some(value.to_string()),
                    memtable::GetResult::Deleted => None,
                    memtable::GetResult::NotFound => find_from_disk(),
                },
                None => find_from_disk(),
            }
        }

        fn flush(
            &mut self,
            memtable_entries: MemtableEntries<String, String>,
        ) -> Result<(), io::Error> {
            self.flushing = Some(memtable_entries);
            let MemtableEntries {
                entries,
                tombstones, // TODO: persist records marked as deleted
            } = self.flushing.as_ref().unwrap();

            let next_data_gen = self.data_gen + 1;
            let new_data_file = DataFile::of(&self.dir_name, next_data_gen);
            let new_index = new_data_file.create(self.flushing.as_ref().unwrap())?;
            let new_index_file = IndexFile::of(next_data_gen, &self.dir_name);
            new_index_file.create_index(&new_index)?;

            println!("entries: {:?}", entries);
            self.data_gen = next_data_gen;
            self.flushing = None;
            Ok(())
        }

        fn clear(&mut self) -> Result<(), io::Error> {
            (0..=self.data_gen).for_each(|gen| {
                DataFile::clear(&self.dir_name, gen);
                IndexFile::clear(gen, &self.dir_name);
            });
            self.data_gen = 0;
            Ok(())
        }
    }
}
