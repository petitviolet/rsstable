//! SSTable(Sorted String Table) in Rust
//! Basically, this is a Key-Value store on top of local file storage.

use std::io;
use log;
mod disktable;
mod memtable;
mod rich_file;

pub struct SSTable {
    // Sorted *String* Table :)
    memtable: Box<dyn memtable::Memtable<Key = String, Value = String>>,
    disktable: Box<dyn disktable::Disktable>,
}

impl SSTable {
    pub fn new(dir_name: &str, mem_max_entry: usize) -> SSTable {
        std::fs::create_dir_all(dir_name).expect(&format!("failed to create directory {}", dir_name));
        SSTable {
            memtable: Box::new(memtable::default::BTreeMemtable::new(
                dir_name,
                mem_max_entry,
            )),
            disktable: Box::new(disktable::default::FileDisktable::new(dir_name).unwrap()),
        }
    }
    pub fn get(&self, key: impl Into<String>) -> Option<String> {
        let key = key.into();
        match self.memtable.get(&key) {
            memtable::GetResult::Found(value) => Some(value.to_string()),
            memtable::GetResult::Deleted => None,
            memtable::GetResult::NotFound => self.disktable.find(&key),
        }
    }
    pub fn insert(
        &mut self,
        key: impl Into<String>,
        value: impl Into<String>,
    ) -> Result<(), io::Error> {
        let key = key.into();
        let value = value.into();
        self.memtable.set(key, value).on_flush(|mem| {
            log::debug!(
                "flush! memtable: {:?}, tombstones: {:?}",
                mem.entries, mem.tombstones
            );
            self.disktable.flush(mem)
        })
    }

    pub fn delete(&mut self, key: impl Into<String>) -> () {
        self.memtable.delete(key.into());
    }

    pub fn clear(&mut self) -> Result<(), io::Error> {
        self.disktable.clear()?;
        self.memtable.clear();
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::sst::SSTable;
    #[test]
    fn test_sstable() {
        let key = |i| format!("key-{}", i);
        let value = |i| format!("value-{}", i);

        let mut sst = SSTable::new("./test_tmp", 200);
        assert!(sst.clear().is_ok());
        // get -> set -> get
        (1..300).for_each(|i| {
            assert_eq!(sst.get(key(i)), None);
            sst.insert(key(i), value(i)).expect("success");
            assert_eq!(sst.get(key(i)), Some(value(i)));
        });
        // get -> delete -> get
        (1..300).for_each(|i| {
            assert_eq!(sst.get(key(i)), Some(value(i)));
            sst.delete(key(i));
            assert_eq!(sst.get(key(i)), None);
        });
        // get
        (1..300).for_each(|i| {
            assert_eq!(sst.get(key(i)), None);
        });
    }

    #[test]
    fn test_sstabl_tombstones() {
        let key = |i| format!("key-{}", i);
        let value = |i| format!("value-{}", i);
        let mut sst = SSTable::new("./test_tmp", 3);
        assert!(sst.clear().is_ok());
        (1..=5).for_each(|i| {
            sst.insert(key(i), value(i)).expect("success");
        });
        sst.delete(key(2));
        // restore WAL
        // memtable: [4, 5], tombstone: [2], disktable: [1, 2, 3]
        let mut sst = SSTable::new("./test_tmp", 3);
        assert_eq!(sst.get(key(1)), Some(value(1)));
        assert_eq!(sst.get(key(2)), None);
        assert_eq!(sst.get(key(3)), Some(value(3)));
        assert_eq!(sst.get(key(4)), Some(value(4)));
        assert_eq!(sst.get(key(5)), Some(value(5)));
    }
} 