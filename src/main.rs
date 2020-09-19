mod sst;
use sst::SSTable;

fn main() {
    let mut sst = SSTable::new("./tmp", 3);
    // sst.clear().expect("failed to clear");
    (1..10).for_each(|i| {
        log::info!("i: {} =====", i);
        let key = || format!("key-{}", i);
        let value = || format!("value-{}", i);
        log::info!("get({}): {:?}", i, sst.get(key()));
        log::info!("insert: {:?}", sst.insert(key(), value()));
        log::info!("get({}): {:?}", i, sst.get(key()));
        log::info!("get({}): {:?}", i + 4, sst.get(format!("key-{}", i + 4)));
    });
    ()
}
