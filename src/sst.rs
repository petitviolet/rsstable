//! SSTable(Sorted String Table) in Rust
//! Basically, this is a Key-Value store on top of local file storage.

use std::{
    io,
};
mod disktable;
mod memtable;

pub struct SSTable {
    // Sorted *String* Table :)
    memtable: Box<dyn memtable::Memtable<Key = String, Value = String>>,
    disktable: Box<dyn disktable::Disktable>,
}
impl SSTable {
    pub fn new(dir_name: &str, mem_max_entry: usize) -> SSTable {
        SSTable {
            memtable: Box::new(memtable::default::HashMemtable::new(mem_max_entry)),
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
            println!(
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
            sst.delete(key(i));
            assert_eq!(sst.get(key(i)), None);
        });
        (1..=10).for_each(|i| {
            assert_eq!(sst.get(key(i)), None);
        });
    }
}
