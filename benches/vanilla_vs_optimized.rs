use std::path::PathBuf;
use std::time::Duration;

use criterion::{criterion_group, criterion_main};
use criterion::{BenchmarkId, Criterion, Throughput};
use noir_compute::data_type::{NoirData, NoirType, NoirTypeRef, Schema};
use noir_compute::optimization::dsl::expressions::*;
use noir_compute::StreamEnvironment;

mod common;
use common::*;
use rand::rngs::ThreadRng;
use rand::RngCore;

fn vanilla_vs_optimized_build(c: &mut Criterion) {
    let mut build_time_group = c.benchmark_group("Vanilla vs Optimized (Build time)");
    build_time_group.sample_size(25);
    build_time_group.measurement_time(Duration::from_secs(60));
    build_time_group.warm_up_time(Duration::from_secs(5));

    for n_row in &[1000, 10000, 100000] {
        for n_col in &[10, 100, 1000] {
            let source_file = PathBuf::from(format!("test_csv/{}_{}.csv", n_row, n_col));
            let size = source_file.metadata().unwrap().len();
            let input = format!("{}x{}", n_row, n_col);
            build_time_group.throughput(Throughput::Bytes(size));

            build_time_group.bench_with_input(
                BenchmarkId::new("Vanilla group-by", &input),
                &source_file,
                |b, path| {
                    b.iter(|| {
                        let mut env = StreamEnvironment::default();
                        env.stream_csv_noirdata(path.to_path_buf())
                            .into_box()
                            .group_by(|item| item[0].floor())
                            .into_box()
                            .filter(|(_, item)| (item[0] / item[1]).round() == NoirType::Int32(1))
                            .into_box()
                            .collect_vec();
                    });
                },
            );

            build_time_group.bench_with_input(
                BenchmarkId::new("Optimized group-by", &input),
                &source_file,
                |b, path| {
                    b.iter(|| {
                        let mut env = StreamEnvironment::default();
                        env.stream_csv_optimized(path.to_path_buf())
                            .with_schema(Schema::same_type(*n_col, NoirTypeRef::Int32))
                            .group_by(col(0).floor())
                            .filter((col(0) / col(1)).round().eq(i(1)))
                            .collect_vec();
                    });
                },
            );

            build_time_group.bench_with_input(
                BenchmarkId::new("Vanilla sum", &input),
                &source_file,
                |b, path| {
                    b.iter(|| {
                        let mut env = StreamEnvironment::default();
                        env.stream_csv_noirdata(path.to_path_buf())
                            .into_box()
                            .filter(|item| item[0].modulo(10) == item[*n_col - 1].modulo(10))
                            .into_box()
                            .map(|item| NoirData::NoirType(item[0] + item[*n_col - 1]))
                            .into_box()
                            .collect_vec();
                    });
                },
            );

            build_time_group.bench_with_input(
                BenchmarkId::new("Optimized sum", &input),
                &source_file,
                |b, path| {
                    b.iter(|| {
                        let mut env = StreamEnvironment::default();
                        env.stream_csv_optimized(path.to_path_buf())
                            .with_schema(Schema::same_type(*n_col, NoirTypeRef::Int32))
                            .filter(col(0).modulo(i(10)).eq(col(*n_col - 1).modulo(i(10))))
                            .select(&[col(0) + col(*n_col - 1)])
                            .collect_vec();
                    });
                },
            );

            build_time_group.bench_with_input(
                BenchmarkId::new("Vanilla join", &input),
                &source_file,
                |b, path| {
                    b.iter(|| {
                        let mut env = StreamEnvironment::default();
                        let left = env.stream_csv_noirdata(path.to_path_buf()).into_box();
                        let right = env.stream_csv_noirdata(path.to_path_buf()).into_box();
                        left.join(
                            right,
                            |item| vec![item[0], item[1]],
                            |item| vec![item[0], item[1]],
                        )
                        .filter(|(_, (item_left, item_right))| item_left[2] != item_right[2])
                        .collect_vec();
                    });
                },
            );

            build_time_group.bench_with_input(
                BenchmarkId::new("Optimized join", &input),
                &source_file,
                |b, path| {
                    b.iter(|| {
                        let mut env = StreamEnvironment::default();
                        let left = env
                            .stream_csv_optimized(path.to_path_buf())
                            .with_schema(Schema::same_type(*n_col, NoirTypeRef::Int32));
                        let right = env
                            .stream_csv_optimized(path.to_path_buf())
                            .with_schema(Schema::same_type(*n_col, NoirTypeRef::Int32));
                        left.join(right, &[col(0), col(1)], &[col(0), col(1)])
                            .filter(col(2).neq(col(n_col + 2)))
                            .collect_vec();
                    });
                },
            );
        }
    }

    build_time_group.finish();
}

fn vanilla_vs_optimized_execution(c: &mut Criterion) {
    let mut execution_time_group = c.benchmark_group("Vanilla vs Optimized (Execution time)");
    execution_time_group.sample_size(25);
    execution_time_group.measurement_time(Duration::from_secs(60));
    execution_time_group.warm_up_time(Duration::from_secs(5));

    for n_row in &[1000, 10000, 100000] {
        for n_col in &[10, 100, 1000] {
            let source_file = PathBuf::from(format!("test_csv/{}_{}.csv", n_row, n_col));
            let size = source_file.metadata().unwrap().len();
            let input = format!("{}x{}", n_row, n_col);
            execution_time_group.throughput(Throughput::Bytes(size));

            execution_time_group.bench_with_input(
                BenchmarkId::new("Vanilla group-by", &input),
                &source_file,
                |b, path| {
                    noir_bench_default(b, |env| {
                        env.stream_csv_noirdata(path.to_path_buf())
                            .into_box()
                            .group_by(|item| item[0].floor())
                            .into_box()
                            .filter(|(_, item)| (item[0] / item[1]).round() == NoirType::Int32(1))
                            .into_box()
                            .collect_vec();
                    });
                },
            );

            execution_time_group.bench_with_input(
                BenchmarkId::new("Optimized group-by", &input),
                &source_file,
                |b, path| {
                    noir_bench_default(b, |env| {
                        env.stream_csv_optimized(path.to_path_buf())
                            .with_schema(Schema::same_type(*n_col, NoirTypeRef::Int32))
                            .group_by(col(0).floor())
                            .filter((col(0) / col(1)).round().eq(i(1)))
                            .collect_vec();
                    });
                },
            );

            execution_time_group.bench_with_input(
                BenchmarkId::new("Vanilla sum", &input),
                &source_file,
                |b, path| {
                    noir_bench_default(b, |env| {
                        env.stream_csv_noirdata(path.to_path_buf())
                            .into_box()
                            .filter(|item| item[0].modulo(10) == item[*n_col - 1].modulo(10))
                            .into_box()
                            .map(|item| NoirData::NoirType(item[0] + item[*n_col - 1]))
                            .into_box()
                            .collect_vec();
                    });
                },
            );

            execution_time_group.bench_with_input(
                BenchmarkId::new("Optimized sum", &input),
                &source_file,
                |b, path| {
                    noir_bench_default(b, |env| {
                        env.stream_csv_optimized(path.to_path_buf())
                            .with_schema(Schema::same_type(*n_col, NoirTypeRef::Int32))
                            .filter(col(0).modulo(i(10)).eq(col(*n_col - 1).modulo(i(10))))
                            .select(&[col(0) + col(*n_col - 1)])
                            .collect_vec();
                    });
                },
            );

            if !input.eq("100000x1000") {
                execution_time_group.bench_with_input(
                    BenchmarkId::new("Vanilla join", &input),
                    &source_file,
                    |b, path| {
                        noir_bench_default(b, |env| {
                            let left = env.stream_csv_noirdata(path.to_path_buf()).into_box();
                            let right = env.stream_csv_noirdata(path.to_path_buf()).into_box();
                            left.join(
                                right,
                                |item| vec![item[0], item[1]],
                                |item| vec![item[0], item[1]],
                            )
                            .filter(|(_, (item_left, item_right))| item_left[2] != item_right[2])
                            .collect_vec();
                        });
                    },
                );
            }

            execution_time_group.bench_with_input(
                BenchmarkId::new("Optimized join", &input),
                &source_file,
                |b, path| {
                    noir_bench_default(b, |env| {
                        let left = env
                            .stream_csv_optimized(path.to_path_buf())
                            .with_schema(Schema::same_type(*n_col, NoirTypeRef::Int32));
                        let right = env
                            .stream_csv_optimized(path.to_path_buf())
                            .with_schema(Schema::same_type(*n_col, NoirTypeRef::Int32));
                        left.join(right, &[col(0), col(1)], &[col(0), col(1)])
                            .filter(col(2).neq(col(n_col + 2)))
                            .collect_vec();
                    });
                },
            );
        }
    }

    execution_time_group.finish();
}

fn random_row(rng: &mut ThreadRng) -> NoirData {
    let col = 5;
    let mut row = Vec::with_capacity(col);
    for _ in 0..col {
        row.push(NoirType::Int32(rng.next_u32() as i32))
    }
    NoirData::Row(row)
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

    let mut g = c.benchmark_group("Expr vs Closures (Execution time)");
    g.sample_size(25);
    g.measurement_time(Duration::from_secs(60));
    g.warm_up_time(Duration::from_secs(5));

    for (size, items) in inputs {
        g.bench_with_input(BenchmarkId::new("Closure", size), &items, |b, items| {
            let closure = |item: &NoirData| item[3].modulo(7) == NoirType::Int32(0);
            b.iter(move || {
                for item in items.iter() {
                    closure(item);
                }
            });
        });

        g.bench_with_input(BenchmarkId::new("Expression", size), &items, |b, items| {
            let expr = col(3).modulo(i(7)).eq(i(0));
            b.iter(move || {
                for item in items.iter() {
                    expr.evaluate(item);
                }
            });
        });
    }

    g.finish();
}

criterion_group!(
    benches,
    vanilla_vs_optimized_build,
    vanilla_vs_optimized_execution,
    expr_vs_closures
);
criterion_main!(benches);
