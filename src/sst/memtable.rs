use std::{io, collections::{BTreeMap, BTreeSet}};

pub trait Memtable {
    type Key;
    type Value;
    fn get(&self, key: &Self::Key) -> Option<&Self::Value>;
    fn set(
        &mut self,
        key: Self::Key,
        value: Self::Value,
    ) -> MemtableOnFlush<Self::Key, Self::Value>;
    fn delete(&mut self, key: Self::Key) -> ();
    fn clear(&mut self) -> ();
}
pub struct MemtableOnFlush<Key, Value> { 
  flushed: Option<(Box<BTreeMap<Key, Value>>,
  Box<BTreeSet<Key>>)>
}

impl<Key, Value> MemtableOnFlush<Key, Value> { 
  pub fn on_flush(self, f: impl FnOnce((
    Box<BTreeMap<Key, Value>>,
    Box<BTreeSet<Key>>,
  )) -> io::Result<()>) -> io::Result<()>
  {
    match self.flushed {
      Some(flushed) => f(flushed),
      None => Ok(())
    }
  }
}

pub mod default {
    use super::{MemtableOnFlush, Memtable};
    use std::{
        collections::{BTreeMap, BTreeSet},
        hash::Hash, io,
    };

    pub struct HashMemtable<K, V> {
        max_entry: usize,
        underlying: BTreeMap<K, V>,
        tombstone: BTreeSet<K>,
    }
    impl<K: Hash + Eq + Ord, V> HashMemtable<K, V> {
        pub fn new(max_entry: usize) -> HashMemtable<K, V> {
            HashMemtable {
                max_entry,
                underlying: BTreeMap::new(),
                tombstone: BTreeSet::new(),
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

        fn flush(&mut self) -> (Box<BTreeMap<K, V>>, Box<BTreeSet<K>>) {
            let contents = std::mem::replace(&mut self.underlying, BTreeMap::new());
            let deleted = std::mem::replace(&mut self.tombstone, BTreeSet::new());
            (Box::new(contents), Box::new(deleted))
        }
    }
    impl<K: Hash + Eq + Ord, V> Memtable for HashMemtable<K, V> {
        type Key = K;
        type Value = V;

        fn get(&self, key: &Self::Key) -> Option<&Self::Value> {
            self.with_check_tombstone(key, || None, || self.underlying.get(&key))
        }

        fn set(
            &mut self,
            key: Self::Key,
            value: Self::Value,
        ) -> MemtableOnFlush<Self::Key, Self::Value> {
            self.tombstone.remove(&key);
            self.underlying.insert(key, value);
            if self.underlying.len() >= self.max_entry {
              MemtableOnFlush { flushed: Some(self.flush()) }
            } else {
              MemtableOnFlush { flushed: None }
            }
        }

        fn delete(&mut self, key: Self::Key) -> () {
            self.underlying.remove(&key);
            self.tombstone.insert(key);
        }
        fn clear(&mut self) -> () {
            self.underlying.clear();
            self.tombstone.clear();
        }
    }
}
