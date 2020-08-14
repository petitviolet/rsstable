//! SSTable(Sorted String Table) in Rust
//! Basically, this is a Key-Value store on top of local file storage.

use std::{
    collections::{BTreeMap, BTreeSet},
    io,
    ops::Deref,
};
mod disktable;
mod memtable;

pub struct SSTable {
    // Sorted *String* Table :)
    memtable: Box<dyn memtable::Memtable<Key = String, Value = String>>,
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
        self.memtable.set(key, value).on_flush(
            |args: (Box<BTreeMap<String, String>>, Box<BTreeSet<String>>)| {
                let (memtable, tombstones) = args;
                println!(
                    "flush! memtable: {:?}, tombstones: {:?}",
                    memtable, tombstones
                );
                self.disktable.flush(memtable, tombstones)
            },
        )
    }

    pub fn clear(&mut self) -> Result<(), io::Error> {
        self.disktable.clear()?;
        self.memtable.clear();
        Ok(())
    }
}

mod tests {
    use crate::sst::SSTable;
    #[test]
    fn test_get_and_set_and_get() {
        let mut sst = SSTable::new("./test_tmp", 3);
        assert!(sst.clear().is_ok());
        let key = |i| format!("key-{}", i);
        let value = |i| format!("value-{}", i);
        (1..=10).for_each(|i| {
          println!("{} ------", i);
          assert_eq!(sst.get(key(i)), None);
          sst.insert(key(i), value(i)).expect("success");
          assert_eq!(sst.get(key(i)), Some(value(i)));
        });
        (1..=10).for_each(|i| {
          assert_eq!(sst.get(key(i)), Some(value(i)));
        });
    }
}
