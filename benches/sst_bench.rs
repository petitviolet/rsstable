#[macro_use]
extern crate criterion;

use criterion::Criterion;
use rsstable::sst::SSTable;
use simple_logger;
fn test_sstable_performance(c: &mut Criterion) {
    let mut sst = SSTable::new("./test_bench", 1);
    simple_logger::SimpleLogger::new()
        .with_level(log::LevelFilter::Info)
        .init()
        .unwrap();
    sst.clear();
    // prepare
    let disk_key = "1";
    let mem_key = "999";
    sst.insert(disk_key, disk_key);
    sst.insert("hoge", "hoge");
    sst.insert(mem_key, mem_key);

    c.bench_function("sstable get from memtable", |b| {
        b.iter(|| {
            sst.get(mem_key)
                .expect(&format!("failed to get value by key {}", mem_key));
        })
    });

    c.bench_function("sstable get from disktable", |b| {
        b.iter(|| {
            sst.get(disk_key)
                .expect(&format!("failed to get value by key {}", disk_key));
        })
    });

    c.bench_function("sstable insert mem", |b| {
        b.iter(|| {
            sst.insert(mem_key, mem_key)
                .expect(&format!("failed to insert value by key {}", mem_key));
        })
    });

    c.bench_function("sstable insert disk", |b| {
        let mut disk = true;
        b.iter(|| {
            if disk {
                sst.insert(disk_key, disk_key)
                    .expect(&format!("failed to insert value by key {}", disk_key));
            } else {
                sst.insert(mem_key, mem_key)
                    .expect(&format!("failed to insert value by key {}", mem_key));
            }
            disk = !disk;
        })
    });
}

criterion_group!(benches, test_sstable_performance);
criterion_main!(benches);
