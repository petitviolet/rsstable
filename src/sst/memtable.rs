mod wal;
use std::{
    collections::{BTreeMap, BTreeSet},
    hash::Hash,
    io,
};

pub(crate) trait Memtable {
    type Key;
    type Value;
    fn get(&self, key: &Self::Key) -> GetResult<&Self::Value>;
    fn set(
        &mut self,
        key: Self::Key,
        value: Self::Value,
    ) -> MemtableOnFlush<Self::Key, Self::Value>;
    fn delete(&mut self, key: Self::Key) -> ();
    fn clear(&mut self) -> ();
    fn restore(&mut self) -> io::Result<()>;
}
pub(crate) enum GetResult<T> {
    Found(T),
    Deleted,
    NotFound,
}
pub(crate) struct MemtableOnFlush<Key, Value> {
    flushed: Option<MemtableEntries<Key, Value>>,
}
pub(crate) struct MemtableEntries<Key, Value> {
    pub entries: Box<BTreeMap<Key, Value>>,
    pub tombstones: Box<BTreeSet<Key>>,
}

impl<K: Hash + Eq + Ord + From<String>, V: From<String>> MemtableEntries<K, V> {
    pub fn get(&self, key: &K) -> GetResult<&V> {
        if self.tombstones.get(key).is_none() {
            self.entries
                .get(key)
                .map(GetResult::Found)
                .unwrap_or(GetResult::NotFound)
        } else {
            GetResult::Deleted
        }
    }
}

impl<Key, Value> MemtableOnFlush<Key, Value> {
    pub fn on_flush(
        self,
        f: impl FnOnce(MemtableEntries<Key, Value>) -> io::Result<()>,
    ) -> io::Result<()> {
        match self.flushed {
            Some(flushed) => f(flushed),
            None => Ok(()),
        }
    }
}

pub(crate) mod default {
    use super::*;
    use std::{
        collections::{BTreeMap, BTreeSet},
        hash::Hash,
    };
    use wal::WriteAheadLog;

    pub struct BTreeMemtable<K, V> {
        max_entry: usize,
        underlying: BTreeMap<K, V>,
        tombstone: BTreeSet<K>,
        wal: WriteAheadLog,
    }
    impl<K: Hash + Eq + Ord + From<String>, V: From<String>> BTreeMemtable<K, V> {
        pub fn new(dir_name: &str, max_entry: usize) -> BTreeMemtable<K, V> {
            let mut underlying = BTreeMap::new();
            let mut tombstone = BTreeSet::new();
            WriteAheadLog::restore(dir_name)
                .expect("failed to load WAL")
                .for_each(|entry| match entry {
                    wal::Entry::Inserted { key, value } => {
                        underlying.insert(From::from(key), From::from(value));
                    }
                    wal::Entry::Deleted { key } => {
                        tombstone.insert(From::from(key));
                    }
                });
            let wal = WriteAheadLog::new(dir_name);
            BTreeMemtable {
                max_entry,
                wal,
                underlying,
                tombstone,
            }
        }

        fn is_deleted(&self, key: &K) -> bool {
            self.tombstone.get(key).is_some()
        }

        fn with_check_tombstone<T>(
            &self,
            key: &K,
            deleted: impl Fn() -> T,
            not_deleted: impl Fn() -> T,
        ) -> T {
            if self.is_deleted(key) {
                deleted()
            } else {
                not_deleted()
            }
        }

        fn flush(&mut self) -> MemtableEntries<K, V> {
            let contents = std::mem::replace(&mut self.underlying, BTreeMap::new());
            let deleted = std::mem::replace(&mut self.tombstone, BTreeSet::new());
            self.wal.clear().expect("failed to clear WAL");
            MemtableEntries {
                entries: Box::new(contents),
                tombstones: Box::new(deleted),
            }
        }
    }

    impl<K: Hash + Eq + Ord + ToString + From<String>, V: ToString + From<String>> Memtable
        for BTreeMemtable<K, V>
    {
        type Key = K;
        type Value = V;

        fn get(&self, key: &Self::Key) -> GetResult<&Self::Value> {
            self.with_check_tombstone(
                key,
                || GetResult::Deleted,
                || {
                    self.underlying
                        .get(&key)
                        .map(GetResult::Found)
                        .unwrap_or(GetResult::NotFound)
                },
            )
        }

        fn set(
            &mut self,
            key: Self::Key,
            value: Self::Value,
        ) -> MemtableOnFlush<Self::Key, Self::Value> {
            self.tombstone.remove(&key);
            self.wal
                .insert((&key.to_string(), &value.to_string()))
                .expect("failed to write WAL");
            self.underlying.insert(key, value);
            if self.underlying.len() >= self.max_entry {
                MemtableOnFlush {
                    flushed: Some(self.flush()),
                }
            } else {
                MemtableOnFlush { flushed: None }
            }
        }
        fn delete(&mut self, key: Self::Key) -> () {
            self.wal
                .delete(&key.to_string())
                .expect("failed to write WAL");
            self.underlying.remove(&key);
            self.tombstone.insert(key);
        }
        fn clear(&mut self) -> () {
            self.wal.clear().expect("failed to clear WAL");
            self.underlying.clear();
            self.tombstone.clear();
        }
        fn restore(&mut self) -> io::Result<()> {
            todo!()
        }
    }
}
