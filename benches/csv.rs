use std::os::unix::prelude::MetadataExt;
use std::path::PathBuf;

use criterion::{criterion_group, criterion_main, Criterion};
use criterion::{BenchmarkId, Throughput};
use noir::StreamEnvironment;

mod common;

fn csv_bench(c: &mut Criterion) {
    let mut g = c.benchmark_group("csv");

    let path = PathBuf::from("csv/1000000_100_m.csv");
    let file_size = path.metadata().unwrap().size();
    g.throughput(Throughput::Bytes(file_size));

    g.bench_with_input(
        BenchmarkId::new("csv-serde", path.file_name().unwrap().to_string_lossy()),
        &path,
        |b, path| {
            b.iter(move || {
                let mut env = StreamEnvironment::default();
                env.stream_csv_noirdata_old(path.to_path_buf())
                    .for_each(|v| {
                        std::hint::black_box(v);
                    });
                env.execute_blocking();
            })
        },
    );

    g.bench_with_input(
        BenchmarkId::new("csv-nom", path.file_name().unwrap().to_string_lossy()),
        &path,
        |b, path| {
            b.iter(move || {
                let mut env = StreamEnvironment::default();
                env.stream_csv_noirdata(path.to_path_buf()).for_each(|v| {
                    std::hint::black_box(v);
                });
                env.execute_blocking();
            })
        },
    );

    g.finish();
}

criterion_group!(benches, csv_bench);
criterion_main!(benches);
