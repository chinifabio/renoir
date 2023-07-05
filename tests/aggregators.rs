use noir::{
    data_type::{NoirData, NoirType},
    operator::source::IteratorSource,
};
use utils::TestHelper;

mod utils;

#[test]
fn group_by_min_element() {
    TestHelper::local_remote_env(|mut env| {
        let source = IteratorSource::new(0..10u8);
        let res = env
            .stream(source)
            .group_by_min_element(|&x| x % 2, |&x| x)
            .collect_vec();
        env.execute_blocking();
        if let Some(mut res) = res.get() {
            res.sort_unstable();
            assert_eq!(res, &[(0, 0), (1, 1)]);
        }
    });
}

#[test]
fn group_by_max_element() {
    TestHelper::local_remote_env(|mut env| {
        let source = IteratorSource::new(0..10u8);
        let res = env
            .stream(source)
            .group_by_max_element(|&x| x % 2, |&x| x)
            .collect_vec();
        env.execute_blocking();
        if let Some(mut res) = res.get() {
            res.sort_unstable();
            assert_eq!(res, &[(0, 8), (1, 9)]);
        }
    });
}

#[test]
#[allow(clippy::identity_op)]
fn group_by_sum() {
    TestHelper::local_remote_env(|mut env| {
        let source = IteratorSource::new(0..10u8);
        let res = env
            .stream(source)
            .group_by_sum(|&x| x % 2, |x| x)
            .collect_vec();
        env.execute_blocking();
        if let Some(mut res) = res.get() {
            res.sort_unstable();
            assert_eq!(res, &[(0, 0 + 2 + 4 + 6 + 8), (1, 1 + 3 + 5 + 7 + 9)]);
        }
    });
}

#[test]
#[allow(clippy::identity_op)]
fn group_by_avg() {
    TestHelper::local_remote_env(|mut env| {
        let source = IteratorSource::new(0..10u8);
        let res = env
            .stream(source)
            .group_by_avg(|&x| x % 2, |&x| x as f64)
            .collect_vec();
        env.execute_blocking();
        if let Some(mut res) = res.get() {
            res.sort_by_key(|(m, _)| *m);
            assert_eq!(
                res,
                &[
                    (0, (0 + 2 + 4 + 6 + 8) as f64 / 5f64),
                    (1, (1 + 3 + 5 + 7 + 9) as f64 / 5f64)
                ]
            );
        }
    });
}

#[test]
fn group_by_count() {
    TestHelper::local_remote_env(|mut env| {
        let source = IteratorSource::new(0..10u8);
        let res = env.stream(source).group_by_count(|&x| x % 2).collect_vec();
        env.execute_blocking();
        if let Some(mut res) = res.get() {
            res.sort_by_key(|(m, _)| *m);
            assert_eq!(res, &[(0, 5), (1, 5)]);
        }
    });
}

#[test]
fn max() {
    TestHelper::local_remote_env(|mut env| {
        let source = IteratorSource::new(vec![1, 2, 0, 5, 9, 3, 7, 6, 4, 8].into_iter());
        let res = env.stream(source).max(|&n| n).collect_vec();
        env.execute_blocking();

        if let Some(res) = res.get() {
            assert_eq!(res, [9u8]);
        }
    });
}

#[test]
fn max_assoc() {
    TestHelper::local_remote_env(|mut env| {
        let source = IteratorSource::new(vec![1, 2, 0, 5, 9, 3, 7, 6, 4, 8].into_iter());
        let res = env.stream(source).max_assoc(|&n| n).collect_vec();
        env.execute_blocking();

        if let Some(res) = res.get() {
            assert_eq!(res, [9u8]);
        }
    });
}

#[test]
fn max_noir_data() {
    TestHelper::local_remote_env(|mut env| {
        let source = IteratorSource::new(
            vec![
                NoirData::NoirType(NoirType::Int32(1)),
                NoirData::NoirType(NoirType::Int32(2)),
                NoirData::NoirType(NoirType::Int32(0)),
                NoirData::NoirType(NoirType::Int32(5)),
            ]
            .into_iter(),
        );
        let res = env.stream(source).max_noir_data(true).collect_vec();
        env.execute_blocking();

        if let Some(res) = res.get() {
            assert_eq!(res, [NoirData::NoirType(NoirType::Int32(5))]);
        }
    });
}

#[test]
fn min() {
    TestHelper::local_remote_env(|mut env| {
        let source = IteratorSource::new(vec![1, 2, 0, 5, 9, 3, 7, 6, 4, 8].into_iter());
        let res = env.stream(source).min(|&n| n).collect_vec();
        env.execute_blocking();

        if let Some(res) = res.get() {
            assert_eq!(res, [0u8]);
        }
    });
}

#[test]
fn min_assoc() {
    TestHelper::local_remote_env(|mut env| {
        let source = IteratorSource::new(vec![1, 2, 0, 5, 9, 3, 7, 6, 4, 8].into_iter());
        let res = env.stream(source).min_assoc(|&n| n).collect_vec();
        env.execute_blocking();

        if let Some(res) = res.get() {
            assert_eq!(res, [0u8]);
        }
    });
}

#[test]
fn mean() {
    TestHelper::local_remote_env(|mut env| {
        let source = IteratorSource::new(0..10);
        let res = env.stream(source).mean(|&n| n as f64).collect_vec();
        env.execute_blocking();

        if let Some(res) = res.get() {
            assert_eq!(res, [4.5]);
        }
    });
}

#[test]
fn mean_noir_data() {
    TestHelper::local_remote_env(|mut env| {
        let rows = vec![
            NoirData::new(
                [
                    NoirType::from(0.0),
                    NoirType::from(8.0),
                    NoirType::from(f32::NAN),
                    NoirType::from(4.0),
                ]
                .to_vec(),
            ),
            NoirData::new(
                [
                    NoirType::from(1.0),
                    NoirType::from(4.0),
                    NoirType::from(f32::NAN),
                    NoirType::from(4.0),
                ]
                .to_vec(),
            ),
            NoirData::new(
                [
                    NoirType::from(f32::NAN),
                    NoirType::from(1.0),
                    NoirType::from(f32::NAN),
                    NoirType::from(9.0),
                ]
                .to_vec(),
            ),
            NoirData::new(
                [
                    NoirType::from(2.0),
                    NoirType::from(9.0),
                    NoirType::from(f32::NAN),
                    NoirType::from(3.0),
                ]
                .to_vec(),
            ),
        ];
        let source = IteratorSource::new(rows.into_iter());
        let res = env.stream(source).mean_noir(true).collect_vec();
        env.execute_blocking();

        if let Some(res) = res.get() {
            assert_eq!(
                res,
                [NoirData::Row(vec![
                    NoirType::from(1.0),
                    NoirType::from(5.5),
                    NoirType::from(None::<f32>),
                    NoirType::from(5.0)
                ])]
            );
        }
    });
}

#[test]
fn mean_noir_data_nan() {
    TestHelper::local_remote_env(|mut env| {
        let rows = vec![
            NoirData::new(
                [
                    NoirType::from(0.0),
                    NoirType::from(f32::NAN),
                    NoirType::from(f32::NAN),
                    NoirType::from(4.0),
                ]
                .to_vec(),
            ),
            NoirData::new(
                [
                    NoirType::from(f32::NAN),
                    NoirType::from(4.0),
                    NoirType::from(f32::NAN),
                    NoirType::from(4.0),
                ]
                .to_vec(),
            ),
        ];
        let source = IteratorSource::new(rows.into_iter());
        let res = env.stream(source).mean_noir(false).collect_vec();
        env.execute_blocking();

        if let Some(res) = res.get() {
            assert_eq!(
                res,
                [NoirData::Row(vec![
                    NoirType::from(f32::NAN),
                    NoirType::from(f32::NAN),
                    NoirType::from(f32::NAN),
                    NoirType::from(4.0)
                ])]
            );
        }
    });
}

#[test]
fn mean_noir_type() {
    TestHelper::local_remote_env(|mut env| {
        let rows = vec![
            NoirData::NoirType(NoirType::from(0.0)),
            NoirData::NoirType(NoirType::from(8.0)),
            NoirData::NoirType(NoirType::from(f32::NAN)),
            NoirData::NoirType(NoirType::from(4.0)),
        ];
        let source = IteratorSource::new(rows.into_iter());
        let res = env.stream(source).mean_noir(true).collect_vec();
        env.execute_blocking();

        if let Some(res) = res.get() {
            assert_eq!(res, [NoirData::NoirType(NoirType::from(4.0))]);
        }
    });
}

#[test]
fn mean_noir_type_nan() {
    TestHelper::local_remote_env(|mut env| {
        let rows = vec![
            NoirData::NoirType(NoirType::from(0.0)),
            NoirData::NoirType(NoirType::from(8.0)),
            NoirData::NoirType(NoirType::from(f32::NAN)),
            NoirData::NoirType(NoirType::from(4.0)),
        ];
        let source = IteratorSource::new(rows.into_iter());
        let res = env.stream(source).mean_noir(false).collect_vec();
        env.execute_blocking();

        if let Some(res) = res.get() {
            assert_eq!(res, [NoirData::NoirType(NoirType::from(f32::NAN))]);
        }
    });
}

#[test]
fn mean_noir_type_none() {
    TestHelper::local_remote_env(|mut env| {
        let rows = vec![
            NoirData::NoirType(NoirType::from(f32::NAN)),
            NoirData::NoirType(NoirType::from(f32::NAN)),
            NoirData::NoirType(NoirType::from(f32::NAN)),
            NoirData::NoirType(NoirType::from(f32::NAN)),
        ];
        let source = IteratorSource::new(rows.into_iter());
        let res = env.stream(source).mean_noir(true).collect_vec();
        env.execute_blocking();

        if let Some(res) = res.get() {
            assert_eq!(res, [NoirData::NoirType(NoirType::from(None::<f32>))]);
        }
    });
}
