use std::path::PathBuf;
use std::time::Duration;

use criterion::{criterion_group, criterion_main};
use criterion::{BenchmarkId, Criterion, Throughput};
use itertools::Itertools;
use noir_compute::data_type::schema::Schema;
use noir_compute::optimization::dsl::expressions::*;

mod common;
use common::*;
use noir_compute::data_type::noir_type::{NoirType, NoirTypeKind};
use noir_compute::data_type::stream_item::StreamItem;
use noir_compute::optimization::dsl::jit::JitCompiler;
use noir_compute::optimization::optimizer::OptimizationOptions;
use rand::rngs::{SmallRng, ThreadRng};
use rand::{Rng, RngCore, SeedableRng};

const SAMPLE_SIZE: usize = 20;
const WARM_UP_TIME: Duration = Duration::from_secs(5);
const MEASUREMENT_TIME: Duration = Duration::from_secs(30);

fn projection_pushdown_filter(c: &mut Criterion) {
    let mut group = c.benchmark_group("ProjectionPushdown(filter)");
    group.sample_size(SAMPLE_SIZE);
    group.measurement_time(MEASUREMENT_TIME);
    group.warm_up_time(WARM_UP_TIME);

    let n_row = 10000;
    for n_col in [10, 100, 1000] {
        let source_file = format!("../py-evaluation/data/{}_{}.csv", n_row, n_col);
        group.throughput(Throughput::Elements(n_row as u64));

        group.bench_with_input(
            BenchmarkId::new("Without", n_col),
            &source_file,
            |b, source_file| {
                noir_bench_default(b, |env| {
                    env.stream_csv_optimized(source_file)
                        .filter(col(0).gte(50))
                        .select([col(1) + col(2)])
                        .without_optimizations()
                        .collect_vec();
                });
            },
        );

        group.bench_with_input(
            BenchmarkId::new("With", n_col),
            &source_file,
            |b, source_file| {
                noir_bench_default(b, |env| {
                    env.stream_csv_optimized(source_file)
                        .filter(col(0).gte(50))
                        .select([col(1) + col(2)])
                        .with_optimizations(
                            OptimizationOptions::none().with_projection_pushdown(true),
                        )
                        .collect_vec();
                });
            },
        );

        group.bench_with_input(
            BenchmarkId::new("Closures", n_col),
            &source_file,
            |b, source_file| {
                noir_bench_default(b, |env| {
                    env.stream_csv_noirdata(source_file.clone())
                        .filter(|row| row[0] >= NoirType::Int32(50))
                        .map(|row| (row[1] + row[2]) / NoirType::Int32(2))
                        .collect_vec();
                });
            },
        );
    }

    group.finish();
}

fn projection_pushdown_groupby(c: &mut Criterion) {
    let mut group = c.benchmark_group("ProjectionPushdown(groupby)");
    group.sample_size(SAMPLE_SIZE);
    group.measurement_time(MEASUREMENT_TIME);
    group.warm_up_time(WARM_UP_TIME);

    let n_row = 10000;
    for n_col in [10, 100, 1000] {
        let source_file = PathBuf::from(format!("../py-evaluation/data/{}_{}.csv", n_row, n_col));
        group.throughput(Throughput::Elements(n_row as u64));

        group.bench_with_input(
            BenchmarkId::new("Without", n_col),
            &source_file,
            |b, source_file| {
                noir_bench_default(b, |env| {
                    env.stream_csv_optimized(source_file)
                        .group_by([col(0) % 5])
                        .select([avg(col(1) + col(2))])
                        .without_optimizations()
                        .collect_vec();
                });
            },
        );

        group.bench_with_input(
            BenchmarkId::new("With", n_col),
            &source_file,
            |b, source_file| {
                noir_bench_default(b, |env| {
                    env.stream_csv_optimized(source_file)
                        .group_by([col(0) % 5])
                        .select([avg(col(1) + col(2))])
                        .with_optimizations(
                            OptimizationOptions::none().with_projection_pushdown(true),
                        )
                        .collect_vec();
                });
            },
        );

        group.bench_with_input(
            BenchmarkId::new("Closures", n_col),
            &source_file,
            |b, source_file| {
                noir_bench_default(b, |env| {
                    env.stream_csv_noirdata(source_file.clone())
                        .group_by(|row| row[0] % NoirType::Int32(5))
                        .fold((NoirType::Int32(0), 0), |acc, value| {
                            acc.0 += value[1] + value[2];
                            acc.1 += 1;
                        })
                        .map(|(_, (sum, count))| sum / NoirType::Int32(count))
                        .collect_vec();
                });
            },
        );
    }

    group.finish();
}

fn projection_pushdown_join(c: &mut Criterion) {
    let mut group = c.benchmark_group("ProjectionPushdown(join)");
    group.sample_size(SAMPLE_SIZE);
    group.measurement_time(MEASUREMENT_TIME);
    group.warm_up_time(WARM_UP_TIME);

    let n_row = 10000;
    for n_col in [10, 100, 1000] {
        let source_file_a = PathBuf::from(format!("../py-evaluation/data/{}_{}.csv", n_row, n_col));
        let source_file_b = PathBuf::from(format!("../py-evaluation/data/{}_{}.csv", 1000, 10));
        group.throughput(Throughput::Elements(n_row as u64));

        group.bench_with_input(
            BenchmarkId::new("Without", n_col),
            &(source_file_a.clone(), source_file_b.clone()),
            |b, (file_a, file_b)| {
                noir_bench_default(b, |env| {
                    let other = env.stream_csv_optimized(file_b);
                    env.stream_csv_optimized(file_a)
                        .join(other, [col(0)], [col(3)])
                        .filter(col(1).gte(col(n_col + 1)))
                        .without_optimizations()
                        .collect_vec();
                });
            },
        );

        group.bench_with_input(
            BenchmarkId::new("With", n_col),
            &(source_file_a.clone(), source_file_b.clone()),
            |b, (file_a, file_b)| {
                noir_bench_default(b, |env| {
                    let other = env
                        .stream_csv_optimized(file_b)
                        .with_schema(Schema::same_type(10, NoirTypeKind::Int32));
                    env.stream_csv_optimized(file_a)
                        .with_schema(Schema::same_type(n_col, NoirTypeKind::Int32))
                        .join(other, [col(0)], [col(3)])
                        .filter(col(1).gte(col(n_col + 1)))
                        .with_optimizations(
                            OptimizationOptions::none().with_projection_pushdown(true),
                        )
                        .collect_vec();
                });
            },
        );

        group.bench_with_input(
            BenchmarkId::new("Closures", n_col),
            &(source_file_a, source_file_b),
            |b, (file_a, file_b)| {
                noir_bench_default(b, |env| {
                    let other = env.stream_csv_noirdata(file_b.clone());
                    env.stream_csv_noirdata(file_a.clone())
                        .join(other, |row| row[0], |row| row[3])
                        .filter(|(_, (left_item, right_item))| left_item[1] >= right_item[0])
                        .collect_vec();
                });
            },
        );
    }

    group.finish();
}

fn predicate_pushdown_groupby_row(c: &mut Criterion) {
    let mut group = c.benchmark_group("PredicatePushdown(groupby)(row)");
    group.sample_size(SAMPLE_SIZE);
    group.measurement_time(MEASUREMENT_TIME);
    group.warm_up_time(WARM_UP_TIME);

    let n_col = 10;
    for n_row in [10_000, 100_000, 1_000_000, 10_000_000] {
        let source_file = PathBuf::from(format!("../py-evaluation/data/{}_{}.csv", n_row, n_col));
        group.throughput(Throughput::Elements(n_row as u64));

        group.bench_with_input(
            BenchmarkId::new("Without", n_row),
            &source_file,
            |b, source_file| {
                noir_bench_default(b, |env| {
                    env.stream_csv_optimized(source_file)
                        .group_by([col(0) % 5])
                        .filter(col(0).gte(50))
                        .without_optimizations()
                        .collect_vec();
                });
            },
        );

        group.bench_with_input(
            BenchmarkId::new("With", n_row),
            &source_file,
            |b, source_file| {
                noir_bench_default(b, |env| {
                    env.stream_csv_optimized(source_file)
                        .group_by([col(0) % 5])
                        .filter(col(0).gte(50))
                        .with_optimizations(
                            OptimizationOptions::none().with_predicate_pushdown(true),
                        )
                        .collect_vec();
                });
            },
        );

        group.bench_with_input(
            BenchmarkId::new("Closures", n_row),
            &source_file,
            |b, source_file| {
                noir_bench_default(b, |env| {
                    env.stream_csv_noirdata(source_file.clone())
                        .group_by(|row| row[0] % NoirType::Int32(5))
                        .filter(|(_, row)| row[0] >= NoirType::Int32(50))
                        .collect_vec();
                });
            },
        );
    }

    group.finish();
}

fn predicate_pushdown_groupby_selectivity(c: &mut Criterion) {
    let mut group = c.benchmark_group("PredicatePushdown(groupby)(selectivity)");
    group.sample_size(SAMPLE_SIZE);
    group.measurement_time(MEASUREMENT_TIME);
    group.warm_up_time(WARM_UP_TIME);

    let n_col = 10;
    let n_row = 100_000;
    for selectivity in [20, 40, 60, 80] {
        let source_file = PathBuf::from(format!("../py-evaluation/data/{}_{}.csv", n_row, n_col));
        group.throughput(Throughput::Elements(n_row as u64));

        group.bench_with_input(
            BenchmarkId::new("Without", selectivity),
            &source_file,
            |b, source_file| {
                noir_bench_default(b, |env| {
                    env.stream_csv_optimized(source_file)
                        .group_by([col(0) % 5])
                        .filter(col(0).gte(selectivity))
                        .without_optimizations()
                        .collect_vec();
                });
            },
        );

        group.bench_with_input(
            BenchmarkId::new("With", selectivity),
            &source_file,
            |b, source_file| {
                noir_bench_default(b, |env| {
                    env.stream_csv_optimized(source_file)
                        .group_by([col(0) % 5])
                        .filter(col(0).gte(selectivity))
                        .with_optimizations(
                            OptimizationOptions::none().with_predicate_pushdown(true),
                        )
                        .collect_vec();
                });
            },
        );

        group.bench_with_input(
            BenchmarkId::new("Closures", selectivity),
            &source_file,
            |b, source_file| {
                noir_bench_default(b, |env| {
                    env.stream_csv_noirdata(source_file.clone())
                        .group_by(|row| row[0] % NoirType::Int32(5))
                        .filter(move |(_, row)| row[0] >= NoirType::Int32(selectivity))
                        .collect_vec();
                });
            },
        );
    }

    group.finish();
}

fn predicate_pushdown_join_row(c: &mut Criterion) {
    let mut group = c.benchmark_group("PredicatePushdown(join)(row)");
    group.sample_size(SAMPLE_SIZE);
    group.measurement_time(MEASUREMENT_TIME);
    group.warm_up_time(WARM_UP_TIME);

    let n_col = 10;
    for n_row in [10_000, 100_000, 1_000_000, 10_000_000] {
        let source_file_a = PathBuf::from(format!("../py-evaluation/data/{}_{}.csv", n_row, n_col));
        let source_file_b = PathBuf::from(format!("../py-evaluation/data/{}_{}.csv", 1000, 10));
        group.throughput(Throughput::Elements(n_row as u64));

        group.bench_with_input(
            BenchmarkId::new("Without", n_row),
            &(source_file_a.clone(), source_file_b.clone()),
            |b, (file_a, file_b)| {
                noir_bench_default(b, |env| {
                    let other = env.stream_csv_optimized(file_a);
                    env.stream_csv_optimized(file_b)
                        .join(other, [col(0)], [col(3)])
                        .filter(col(0).gte(50).and(col(n_col).gte(50)))
                        .without_optimizations()
                        .collect_vec();
                });
            },
        );

        group.bench_with_input(
            BenchmarkId::new("With", n_row),
            &(source_file_a.clone(), source_file_b.clone()),
            |b, (file_a, file_b)| {
                noir_bench_default(b, |env| {
                    let other = env
                        .stream_csv_optimized(file_a)
                        .with_schema(Schema::same_type(n_col, NoirTypeKind::Int32));
                    env.stream_csv_optimized(file_b)
                        .with_schema(Schema::same_type(n_col, NoirTypeKind::Int32))
                        .join(other, [col(0)], [col(3)])
                        .filter(col(0).gte(50).and(col(n_col).gte(50)))
                        .with_optimizations(
                            OptimizationOptions::none().with_predicate_pushdown(true),
                        )
                        .collect_vec();
                });
            },
        );

        group.bench_with_input(
            BenchmarkId::new("Closures", n_row),
            &(source_file_a, source_file_b),
            |b, (file_a, file_b)| {
                noir_bench_default(b, |env| {
                    let other = env.stream_csv_noirdata(file_a.clone());
                    env.stream_csv_noirdata(file_b.clone())
                        .join(other, |row| row[0], |row| row[3])
                        .filter(move |(_, (left_item, right_item))| {
                            left_item[0] >= NoirType::Int32(50)
                                && right_item[n_col] >= NoirType::Int32(50)
                        })
                        .collect_vec();
                });
            },
        );
    }

    group.finish();
}

fn predicate_pushdown_join_selectivity(c: &mut Criterion) {
    let mut group = c.benchmark_group("PredicatePushdown(join)(selectivity)");
    group.sample_size(SAMPLE_SIZE);
    group.measurement_time(MEASUREMENT_TIME);
    group.warm_up_time(WARM_UP_TIME);

    let n_col = 10;
    let n_row = 100_000;
    for selectivity in [20, 40, 60, 80] {
        let source_file_a = PathBuf::from(format!("../py-evaluation/data/{}_{}.csv", n_row, n_col));
        let source_file_b = PathBuf::from(format!("../py-evaluation/data/{}_{}.csv", 1000, 10));
        group.throughput(Throughput::Elements(n_row as u64));

        group.bench_with_input(
            BenchmarkId::new("Without", selectivity),
            &(source_file_a.clone(), source_file_b.clone()),
            |b, (file_a, file_b)| {
                noir_bench_default(b, |env| {
                    let other = env.stream_csv_optimized(file_a);
                    env.stream_csv_optimized(file_b)
                        .join(other, [col(0)], [col(3)])
                        .filter(col(0).gte(selectivity).and(col(n_col).gte(selectivity)))
                        .without_optimizations()
                        .collect_vec();
                });
            },
        );

        group.bench_with_input(
            BenchmarkId::new("With", selectivity),
            &(source_file_a.clone(), source_file_b.clone()),
            |b, (file_a, file_b)| {
                noir_bench_default(b, |env| {
                    let other = env
                        .stream_csv_optimized(file_a)
                        .with_schema(Schema::same_type(n_col, NoirTypeKind::Int32));
                    env.stream_csv_optimized(file_b)
                        .with_schema(Schema::same_type(n_col, NoirTypeKind::Int32))
                        .join(other, [col(0)], [col(3)])
                        .filter(col(0).gte(selectivity).and(col(n_col).gte(selectivity)))
                        .with_optimizations(
                            OptimizationOptions::none().with_predicate_pushdown(true),
                        )
                        .collect_vec();
                });
            },
        );

        group.bench_with_input(
            BenchmarkId::new("Closures", selectivity),
            &(source_file_a, source_file_b),
            |b, (file_a, file_b)| {
                noir_bench_default(b, |env| {
                    let other = env.stream_csv_noirdata(file_a.clone());
                    env.stream_csv_noirdata(file_b.clone())
                        .join(other, |row| row[0], |row| row[3])
                        .filter(move |(_, (left_item, right_item))| {
                            left_item[0] >= NoirType::Int32(selectivity)
                                && right_item[n_col] >= NoirType::Int32(selectivity)
                        })
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
    StreamItem::new(row)
}

fn expression_comparison(c: &mut Criterion) {
    let mut rng = rand::thread_rng();

    let inputs: Vec<_> = [1, 10, 100, 1000]
        .into_iter()
        .map(|size| {
            let mut data = Vec::with_capacity(size);
            for _ in 0..size {
                data.push(random_row(&mut rng))
            }
            (size, data)
        })
        .collect();

    let mut group = c.benchmark_group("Expression (execution time)");
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

        group.bench_with_input(
            BenchmarkId::new("Compiled Expression", size),
            &items,
            |b, items| {
                let expr = col(3).modulo(7).eq(0);
                let schema = Schema::same_type(5, NoirTypeKind::Int32);
                let compiled_expr = expr.compile(&schema, &mut JitCompiler::default());
                b.iter(move || {
                    for item in items.iter() {
                        compiled_expr.evaluate(item);
                    }
                });
            },
        );
    }

    group.finish();
}

fn is_compiled_faster(c: &mut Criterion) {
    unsafe { backtrace_on_stack_overflow::enable() };
    let mut group = c.benchmark_group("Compiled vs Interpreted");
    group.sample_size(SAMPLE_SIZE);
    group.measurement_time(MEASUREMENT_TIME);
    group.warm_up_time(WARM_UP_TIME);

    let n_col = 5;
    for n_row in [100000, 1000000, 10000000, 100000000] {
        group.throughput(Throughput::Elements(n_row));

        group.bench_function(BenchmarkId::new("Interpreted", n_row), |b| {
            noir_bench_default(b, |env| {
                let s = env
                    .stream_par_iter(move |_i, n| {
                        let mut rng = SmallRng::seed_from_u64(0xdeadbeef);
                        (0..(n_row / n))
                            .map(move |_| {
                                (0..n_col)
                                    .map(|_| rng.gen_range(0..100))
                                    .map(NoirType::Int32)
                                    .collect_vec()
                            })
                            .map(StreamItem::from)
                    })
                    .into_box();
                env.optimized_from_stream(s, Schema::same_type(5, NoirTypeKind::Int32))
                    .filter(((col(0) + col(1)) / 2).gte(50))
                    .collect_vec();
            })
        });

        group.bench_function(BenchmarkId::new("Compiled", n_row), |b| {
            noir_bench_default(b, |env| {
                let s = env
                    .stream_par_iter(move |_i, n| {
                        let mut rng = SmallRng::seed_from_u64(0xdeadbeef);
                        (0..(n_row / n))
                            .map(move |_| {
                                (0..n_col)
                                    .map(|_| rng.gen_range(0..100))
                                    .map(NoirType::Int32)
                                    .collect_vec()
                            })
                            .map(StreamItem::from)
                    })
                    .into_box();
                env.optimized_from_stream(s, Schema::same_type(5, NoirTypeKind::Int32))
                    .with_compiled_expressions(true)
                    .filter(((col(0) + col(1)) / 2).gte(50))
                    .collect_vec();
            })
        });
    }

    group.finish();
}

criterion_group!(
    benches,
    projection_pushdown_filter,
    projection_pushdown_groupby,
    projection_pushdown_join,
    predicate_pushdown_groupby_row,
    predicate_pushdown_groupby_selectivity,
    predicate_pushdown_join_row,
    predicate_pushdown_join_selectivity,
    expression_comparison,
    is_compiled_faster
);
criterion_main!(benches);
