use noir::{
    data_type::{NoirData, NoirType},
    operator::source::IteratorSource,
};
use utils::TestHelper;

mod utils;

fn round(x: f32, decimals: u32) -> f32 {
    let y = 10i32.pow(decimals) as f32;
    (x * y).round() / y
}

#[test]
fn skew() {
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
        let res = env.stream(source).skewness(true).collect_vec();
        env.execute_blocking();

        if let Some(res) = res.get() {
            let mut row = res[0].clone().to_row();

            for v in row.iter_mut() {
                if let NoirType::Float32(a) = v {
                    *v = NoirType::from(round(*a, 4))
                }
            }

            let data = [NoirData::Row(row)];

            assert_eq!(
                data,
                [NoirData::Row(vec![
                    NoirType::from(0.0),
                    NoirType::from(-0.1781),
                    NoirType::from(None::<f32>),
                    NoirType::from(0.6798)
                ])]
            );
        }
    });
}

#[test]
fn skew_noir_data_nan() {
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
        let res = env.stream(source).skewness(false).collect_vec();
        env.execute_blocking();

        if let Some(res) = res.get() {
            assert_eq!(
                res,
                [NoirData::Row(vec![
                    NoirType::from(f32::NAN),
                    NoirType::from(f32::NAN),
                    NoirType::from(f32::NAN),
                    NoirType::from(0.0)
                ])]
            );
        }
    });
}

#[test]
fn skew_noir_type() {
    TestHelper::local_remote_env(|mut env| {
        let rows = vec![
            NoirData::NoirType(NoirType::from(0.0)),
            NoirData::NoirType(NoirType::from(8.0)),
            NoirData::NoirType(NoirType::from(f32::NAN)),
            NoirData::NoirType(NoirType::from(4.0)),
        ];
        let source = IteratorSource::new(rows.into_iter());
        let res = env.stream(source).skewness(true).collect_vec();
        env.execute_blocking();

        if let Some(res) = res.get() {
            assert_eq!(res, [NoirData::NoirType(NoirType::from(0.0))]);
        }
    });
}

#[test]
fn skew_noir_type_nan() {
    TestHelper::local_remote_env(|mut env| {
        let rows = vec![
            NoirData::NoirType(NoirType::from(0.0)),
            NoirData::NoirType(NoirType::from(8.0)),
            NoirData::NoirType(NoirType::from(f32::NAN)),
            NoirData::NoirType(NoirType::from(4.0)),
        ];
        let source = IteratorSource::new(rows.into_iter());
        let res = env.stream(source).skewness(false).collect_vec();
        env.execute_blocking();

        if let Some(res) = res.get() {
            assert_eq!(res, [NoirData::NoirType(NoirType::from(f32::NAN))]);
        }
    });
}

#[test]
fn skew_noir_type_none() {
    TestHelper::local_remote_env(|mut env| {
        let rows = vec![
            NoirData::NoirType(NoirType::from(f32::NAN)),
            NoirData::NoirType(NoirType::from(f32::NAN)),
            NoirData::NoirType(NoirType::from(f32::NAN)),
            NoirData::NoirType(NoirType::from(f32::NAN)),
        ];
        let source = IteratorSource::new(rows.into_iter());
        let res = env.stream(source).skewness(true).collect_vec();
        env.execute_blocking();

        if let Some(res) = res.get() {
            assert_eq!(res, [NoirData::NoirType(NoirType::from(None::<f32>))]);
        }
    });
}

#[test]
fn kurt() {
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
        let res = env.stream(source).kurtosis(true).collect_vec();
        env.execute_blocking();

        if let Some(res) = res.get() {
            let mut row = res[0].clone().to_row();

            for v in row.iter_mut() {
                if let NoirType::Float32(a) = v {
                    *v = NoirType::from(round(*a, 4))
                }
            }

            let data = [NoirData::Row(row)];

            assert_eq!(
                data,
                [NoirData::Row(vec![
                    NoirType::from(0.6667),
                    NoirType::from(0.8088),
                    NoirType::from(None::<f32>),
                    NoirType::from(1.2738)
                ])]
            );
        }
    });
}

#[test]
fn kurt_noir_data_nan() {
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
        let res = env.stream(source).kurtosis(false).collect_vec();
        env.execute_blocking();

        if let Some(res) = res.get() {
            assert_eq!(
                res,
                [NoirData::Row(vec![
                    NoirType::from(f32::NAN),
                    NoirType::from(f32::NAN),
                    NoirType::from(f32::NAN),
                    NoirType::from(0.0)
                ])]
            );
        }
    });
}

#[test]
fn kurt_noir_type() {
    TestHelper::local_remote_env(|mut env| {
        let rows = vec![
            NoirData::NoirType(NoirType::from(0.0)),
            NoirData::NoirType(NoirType::from(8.0)),
            NoirData::NoirType(NoirType::from(f32::NAN)),
            NoirData::NoirType(NoirType::from(4.0)),
        ];
        let source = IteratorSource::new(rows.into_iter());
        let res = env.stream(source).kurtosis(true).collect_vec();
        env.execute_blocking();

        if let Some(res) = res.get() {
            let mut v = res[0].clone().to_type();

            if let NoirType::Float32(a) = v {
                v = NoirType::from(round(a, 4))
            }

            let data = [NoirData::NoirType(v)];

            assert_eq!(data, [NoirData::NoirType(NoirType::from(0.6667))]);
        }
    });
}

#[test]
fn kurt_noir_type_nan() {
    TestHelper::local_remote_env(|mut env| {
        let rows = vec![
            NoirData::NoirType(NoirType::from(0.0)),
            NoirData::NoirType(NoirType::from(8.0)),
            NoirData::NoirType(NoirType::from(f32::NAN)),
            NoirData::NoirType(NoirType::from(4.0)),
        ];
        let source = IteratorSource::new(rows.into_iter());
        let res = env.stream(source).kurtosis(false).collect_vec();
        env.execute_blocking();

        if let Some(res) = res.get() {
            assert_eq!(res, [NoirData::NoirType(NoirType::from(f32::NAN))]);
        }
    });
}

#[test]
fn kurt_noir_type_none() {
    TestHelper::local_remote_env(|mut env| {
        let rows = vec![
            NoirData::NoirType(NoirType::from(f32::NAN)),
            NoirData::NoirType(NoirType::from(f32::NAN)),
            NoirData::NoirType(NoirType::from(f32::NAN)),
            NoirData::NoirType(NoirType::from(f32::NAN)),
        ];
        let source = IteratorSource::new(rows.into_iter());
        let res = env.stream(source).kurtosis(true).collect_vec();
        env.execute_blocking();

        if let Some(res) = res.get() {
            assert_eq!(res, [NoirData::NoirType(NoirType::from(None::<f32>))]);
        }
    });
}

#[test]
fn kurtosis_unbiased() {
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
        let res = env.stream(source).kurtosis_unbiased(true).collect_vec();
        env.execute_blocking();

        if let Some(res) = res.get() {
            let mut row = res[0].clone().to_row();

            for v in row.iter_mut() {
                if let NoirType::Float32(a) = v {
                    *v = NoirType::from(round(*a, 4))
                }
            }

            let data = [NoirData::Row(row)];

            assert_eq!(
                data,
                [NoirData::Row(vec![
                    NoirType::from(-1.2893),
                    NoirType::from(-1.1179),
                    NoirType::from(None::<f32>),
                    NoirType::from(4.2252)
                ])]
            );
        }
    });
}

#[test]
fn kurtosis_unbiased_noir_data_nan() {
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
            NoirData::new(
                [
                    NoirType::from(f32::NAN),
                    NoirType::from(4.0),
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
                    NoirType::from(2.0),
                ]
                .to_vec(),
            ),
        ];
        let source = IteratorSource::new(rows.into_iter());
        let res = env.stream(source).kurtosis_unbiased(false).collect_vec();
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
fn kurtosis_unbiased_noir_type() {
    TestHelper::local_remote_env(|mut env| {
        let rows = vec![
            NoirData::NoirType(NoirType::from(0.0)),
            NoirData::NoirType(NoirType::from(8.0)),
            NoirData::NoirType(NoirType::from(f32::NAN)),
            NoirData::NoirType(NoirType::from(4.0)),
            NoirData::NoirType(NoirType::from(4.0)),
            NoirData::NoirType(NoirType::from(4.0)),
        ];
        let source = IteratorSource::new(rows.into_iter());
        let res = env.stream(source).kurtosis_unbiased(true).collect_vec();
        env.execute_blocking();

        if let Some(res) = res.get() {
            let mut v = res[0].clone().to_type();

            if let NoirType::Float32(a) = v {
                v = NoirType::from(round(a, 4))
            }

            let data = [NoirData::NoirType(v)];

            assert_eq!(data, [NoirData::NoirType(NoirType::from(2.0))]);
        }
    });
}

#[test]
fn kurtosis_unbiased_noir_type_nan() {
    TestHelper::local_remote_env(|mut env| {
        let rows = vec![
            NoirData::NoirType(NoirType::from(0.0)),
            NoirData::NoirType(NoirType::from(8.0)),
            NoirData::NoirType(NoirType::from(f32::NAN)),
            NoirData::NoirType(NoirType::from(4.0)),
        ];
        let source = IteratorSource::new(rows.into_iter());
        let res = env.stream(source).kurtosis_unbiased(false).collect_vec();
        env.execute_blocking();

        if let Some(res) = res.get() {
            assert_eq!(res, [NoirData::NoirType(NoirType::from(f32::NAN))]);
        }
    });
}

#[test]
fn kurtosis_unbiased_noir_type_none() {
    TestHelper::local_remote_env(|mut env| {
        let rows = vec![
            NoirData::NoirType(NoirType::from(f32::NAN)),
            NoirData::NoirType(NoirType::from(f32::NAN)),
            NoirData::NoirType(NoirType::from(f32::NAN)),
            NoirData::NoirType(NoirType::from(f32::NAN)),
        ];
        let source = IteratorSource::new(rows.into_iter());
        let res = env.stream(source).kurtosis_unbiased(true).collect_vec();
        env.execute_blocking();

        if let Some(res) = res.get() {
            assert_eq!(res, [NoirData::NoirType(NoirType::from(None::<f32>))]);
        }
    });
}
