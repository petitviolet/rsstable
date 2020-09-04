#[macro_use]
extern crate criterion;

use criterion::Criterion;
use criterion::black_box;
use rsstable::sst::SSTable;
fn test_sstable_insert(c: &mut Criterion) {
    let mut sst = SSTable::new("./test_bench", 300);
    c.bench_function("sstable insert", |b| {
      b.iter(|| {
        (1..1000).for_each(|i| {
            sst.insert(i.to_string(), i.to_string());
        })
      })
    });
}

criterion_group!(benches, test_sstable_insert);
criterion_main!(benches);
