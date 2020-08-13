# Sorted String Table implementation in Rust

For my learning Rust :).

```rust
let mut sst = SSTable::new("./tmp", 3);
let key = "my-key";
let value = "my-value";
println!("get: {}", sst.get(key)); // None
sst.insert(key, value);
println!("get: {}", sst.get(key)); // Some("my-value")
```

- memtable
    - in-memory data structure
    - when number of records exceeds given threshold, dump data into disktable
- disktable
    - rather old data persisted in disk
