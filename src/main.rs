mod sst;
use sst::SSTable;

fn main() {
    let mut sst = SSTable::new("./tmp", 3);
    // sst.clear().expect("failed to clear");
    (1..10).for_each(|i| {
        println!("i: {} =====", i);
        let key = || format!("key-{}", i);
        let value = || format!("value-{}", i);
        println!("get({}): {:?}", i, sst.get(key()));
        println!("insert: {:?}", sst.insert(key(), value()));
        println!("get({}): {:?}", i, sst.get(key()));
        println!("get({}): {:?}", i + 4, sst.get(format!("key-{}", i + 4)));
    });
    ()
}
