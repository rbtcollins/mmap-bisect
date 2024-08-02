use std::fs::OpenOptions;

use criterion::{black_box, criterion_group, criterion_main, Criterion};
use mmap_btree::SST;
use rand::Rng;

/// Benches lookups in a SST file at data/output.sst
pub fn bench_lookup(c: &mut Criterion) {
    let f = OpenOptions::new()
        .read(true)
        .open("data/output.sst")
        .unwrap();
    let sst = SST::new(f).unwrap();
    let mut rnd = rand::thread_rng();
    c.bench_function("lookup 1", |b| {
        b.iter(|| {
            black_box(sst.find(rnd.gen()));
        })
    });
    c.bench_function("make random key", |b| {
        b.iter(|| black_box(rnd.gen::<u32>()))
    });
}

criterion_group!(benches, bench_lookup);
criterion_main!(benches);
