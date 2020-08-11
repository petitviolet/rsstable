//! SSTable(Sorted String Table) in Rust
//! Basically, this is a Key-Value store on top of local file storage.

use crate::sst::memtable::Memtable;
use std::{collections::HashMap, fs, io, ops::Deref, path::Path};
mod disktable;
mod memtable;

pub struct SSTable {
    // memtable: Box<dyn memtable::Memtable<Key = String, Value = String>>,
    memtable: Box<memtable::default::HashMemtable<String, String>>,
    disktable: Box<dyn disktable::Disktable>,
}

impl SSTable {
    pub fn new(dir_name: impl Into<String>, mem_max_entry: usize) -> SSTable {
        SSTable {
            memtable: Box::new(memtable::default::HashMemtable::new(mem_max_entry)),
            disktable: Box::new(disktable::default::FileDisktable::new(dir_name.into()).unwrap()),
        }
    }
    pub fn get(&self, key: impl Into<String>) -> Option<String> {
        let key = key.into();
        self.memtable
            .get(&key)
            .map(|res| res.to_string())
            .or_else(|| self.disktable.find(&key))
    }
    pub fn insert(
        &mut self,
        key: impl Into<String>,
        value: impl Into<String>,
    ) -> Result<(), io::Error> {
        let key = key.into();
        let value = value.into();
        match self.memtable.set(key, value) {
            Some((memtable, tombstones)) => {
                println!(
                    "flush! memtable: {:?}, tombstones: {:?}",
                    memtable, tombstones
                );
                self.disktable.flush(memtable.deref(), tombstones.deref())
            }
            None => Ok(()),
        }
    }

    pub fn clear(&mut self) -> Result<(), io::Error> {
        self.disktable.clear()?;
        self.memtable.clear();
        Ok(())
    }
}
