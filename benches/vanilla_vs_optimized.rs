use std::path::PathBuf;
use std::time::Duration;

use criterion::{criterion_group, criterion_main};
use criterion::{BenchmarkId, Criterion, Throughput};
use noir_compute::data_type::{NoirData, NoirType, NoirTypeKind, Schema, StreamItem};
use noir_compute::optimization::dsl::expressions::*;

mod common;
use common::*;
use rand::rngs::ThreadRng;
use rand::RngCore;

const SAMPLE_SIZE: usize = 25;
const WARM_UP_TIME: Duration = Duration::from_secs(5);
const MEASUREMENT_TIME: Duration = Duration::from_secs(10);

fn predicate_pushdown(c: &mut Criterion) {
    let mut group = c.benchmark_group("Predicate pushdown");
    group.sample_size(SAMPLE_SIZE);
    group.measurement_time(MEASUREMENT_TIME);
    group.warm_up_time(WARM_UP_TIME);

    let n_col = 100;
    for n_row in [1000, 10000, 100000] {
        let source_file = PathBuf::from(format!("test_csv/{}_{}.csv", n_row, n_col));
        group.throughput(Throughput::Elements(n_row as u64));

        group.bench_with_input(
            BenchmarkId::new("Vanilla filter > select", n_col),
            &source_file,
            |b, path| {
                noir_bench_default(b, |env| {
                    env.stream_csv_noirdata(path.to_path_buf())
                        .filter(|item| item[0].modulo(10) == item[99].modulo(10))
                        .map(|item| NoirData::NoirType(item[0] + item[99]))
                        .collect_vec();
                });
            },
        );

        group.bench_with_input(
            BenchmarkId::new("Optimized filter > select", n_col),
            &source_file,
            |b, path| {
                noir_bench_default(b, |env| {
                    env.stream_csv_optimized(path.to_path_buf())
                        .with_schema(Schema::same_type(100, NoirTypeKind::Int32))
                        .filter(col(0).modulo(i(10)).eq(col(99).modulo(i(10))))
                        .select(&[col(0) + col(99)])
                        .collect_vec();
                });
            },
        );

        group.bench_with_input(
            BenchmarkId::new("Vanilla group-by", n_col),
            &source_file,
            |b, path| {
                noir_bench_default(b, |env| {
                    env.stream_csv_noirdata(path.to_path_buf())
                        .group_by(|item| item[0].floor())
                        .filter(|(_, item)| (item[0] / item[1]).round() == NoirType::Int32(1))
                        .collect_vec();
                });
            },
        );

        group.bench_with_input(
            BenchmarkId::new("Optimized group-by", n_col),
            &source_file,
            |b, path| {
                noir_bench_default(b, |env| {
                    env.stream_csv_optimized(path.to_path_buf())
                        .with_schema(Schema::same_type(n_col, NoirTypeKind::Int32))
                        .group_by([col(0).floor()])
                        .filter((col(0) / col(1)).round().eq(i(1)))
                        .collect_vec();
                });
            },
        );

        group.bench_with_input(
            BenchmarkId::new("Vanilla join > filter", n_col),
            &source_file,
            |b, path| {
                noir_bench_default(b, |env| {
                    let left = env.stream_csv_noirdata(path.to_path_buf());
                    let right = env.stream_csv_noirdata(path.to_path_buf());
                    left.join(
                        right,
                        |item| vec![item[0], item[1]],
                        |item| vec![item[0], item[1]],
                    )
                    .filter(|(_, (item_left, item_right))| {
                        item_left[2] == NoirType::Int32(10) && item_right[2] == NoirType::Int32(10)
                    })
                    .collect_vec();
                });
            },
        );

        group.bench_with_input(
            BenchmarkId::new("Optimize join > filter", n_col),
            &source_file,
            |b, path| {
                noir_bench_default(b, |env| {
                    let left = env
                        .stream_csv_optimized(path.to_path_buf())
                        .with_schema(Schema::same_type(100, NoirTypeKind::Int32));
                    let right = env
                        .stream_csv_optimized(path.to_path_buf())
                        .with_schema(Schema::same_type(100, NoirTypeKind::Int32));
                    left.join(right, &[col(0), col(1)], &[col(0), col(1)])
                        .filter(col(2).eq(i(10)).and(col(102).eq(i(10))))
                        .collect_vec();
                });
            },
        );
    }

    group.finish();
}

fn projection_pushdown(c: &mut Criterion) {
    let mut group = c.benchmark_group("Predicate pushdown");
    group.sample_size(SAMPLE_SIZE);
    group.measurement_time(MEASUREMENT_TIME);
    group.warm_up_time(WARM_UP_TIME);

    let n_row = 100000;
    for n_col in &[10, 100, 1000] {
        let source_file = PathBuf::from(format!("test_csv/{}_{}.csv", n_row, n_col));
        group.throughput(Throughput::Elements(*n_col as u64));

        group.bench_with_input(
            BenchmarkId::new("Vanilla filter > select", n_col),
            &source_file,
            |b, path| {
                noir_bench_default(b, |env| {
                    env.stream_csv_noirdata(path.to_path_buf())
                        .filter(|item| item[0].modulo(10) == item[*n_col - 1].modulo(10))
                        .map(|item| NoirData::NoirType(item[0] + item[*n_col - 1]))
                        .collect_vec();
                });
            },
        );

        group.bench_with_input(
            BenchmarkId::new("Optimized filter > select", n_col),
            &source_file,
            |b, path| {
                noir_bench_default(b, |env| {
                    env.stream_csv_optimized(path.to_path_buf())
                        .with_schema(Schema::same_type(*n_col, NoirTypeKind::Int32))
                        .filter(col(0).modulo(i(10)).eq(col(n_col - 1).modulo(i(10))))
                        .select(&[col(0) + col(n_col - 1)])
                        .collect_vec();
                });
            },
        );

        group.bench_with_input(
            BenchmarkId::new("Vanilla group-by", n_col),
            &source_file,
            |b, path| {
                noir_bench_default(b, |env| {
                    env.stream_csv_noirdata(path.to_path_buf())
                        .group_by(|item| item[0].floor())
                        .filter(|(_, item)| (item[0] / item[1]).round() == NoirType::Int32(1))
                        .collect_vec();
                });
            },
        );

        group.bench_with_input(
            BenchmarkId::new("Optimized group-by", n_col),
            &source_file,
            |b, path| {
                noir_bench_default(b, |env| {
                    env.stream_csv_optimized(path.to_path_buf())
                        .with_schema(Schema::same_type(*n_col, NoirTypeKind::Int32))
                        .group_by([col(0).floor()])
                        .filter((col(0) / col(1)).round().eq(i(1)))
                        .collect_vec();
                });
            },
        );

        group.bench_with_input(
            BenchmarkId::new("Vanilla join > filter", n_col),
            &source_file,
            |b, path| {
                noir_bench_default(b, |env| {
                    let left = env.stream_csv_noirdata(path.to_path_buf());
                    let right = env.stream_csv_noirdata(path.to_path_buf());
                    left.join(
                        right,
                        |item| vec![item[0], item[1]],
                        |item| vec![item[0], item[1]],
                    )
                    .filter(|(_, (item_left, item_right))| {
                        item_left[2] == NoirType::Int32(10) && item_right[2] == NoirType::Int32(10)
                    })
                    .collect_vec();
                });
            },
        );

        group.bench_with_input(
            BenchmarkId::new("Optimize join > filter", n_col),
            &source_file,
            |b, path| {
                noir_bench_default(b, |env| {
                    let left = env
                        .stream_csv_optimized(path.to_path_buf())
                        .with_schema(Schema::same_type(*n_col, NoirTypeKind::Int32));
                    let right = env
                        .stream_csv_optimized(path.to_path_buf())
                        .with_schema(Schema::same_type(*n_col, NoirTypeKind::Int32));
                    left.join(right, &[col(0), col(1)], &[col(0), col(1)])
                        .filter(col(2).eq(i(10)).and(col(102).eq(i(10))))
                        .collect_vec();
                });
            },
        );
    }

    group.finish();
}

fn random_row(rng: &mut ThreadRng) -> StreamItem {
    let col = 5;
    let mut row = Vec::with_capacity(col);
    for _ in 0..col {
        row.push(NoirType::Int32(rng.next_u32() as i32))
    }
    StreamItem::DataItem(NoirData::Row(row))
}

fn expr_vs_closures(c: &mut Criterion) {
    let mut rng = rand::thread_rng();

    let inputs: Vec<_> = [1, 50, 100, 1000]
        .into_iter()
        .map(|size| {
            let mut data = Vec::with_capacity(size);
            for _ in 0..size {
                data.push(random_row(&mut rng))
            }
            (size, data)
        })
        .collect();

    let mut group = c.benchmark_group("Expr vs Closures (Execution time)");
    group.sample_size(SAMPLE_SIZE);
    group.measurement_time(MEASUREMENT_TIME);
    group.warm_up_time(WARM_UP_TIME);

    for (size, items) in inputs {
        group.bench_with_input(BenchmarkId::new("Closure", size), &items, |b, items| {
            let closure = |item: &StreamItem| item[3].modulo(7) == NoirType::Int32(0);
            b.iter(move || {
                for item in items.iter() {
                    closure(item);
                }
            });
        });

        group.bench_with_input(BenchmarkId::new("Expression", size), &items, |b, items| {
            let expr = col(3).modulo(i(7)).eq(i(0));
            b.iter(move || {
                for item in items.iter() {
                    expr.evaluate(item);
                }
            });
        });
    }

    group.finish();
}

criterion_group!(
    benches,
    predicate_pushdown,
    projection_pushdown,
    expr_vs_closures
);
criterion_main!(benches);
