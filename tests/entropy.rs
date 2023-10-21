use std::vec;

use noir::{
    data_type::{NoirData, NoirType},
    operator::source::IteratorSource,
};
use utils::TestHelper;

mod utils;

#[test]
fn entropy_noir_data() {
    TestHelper::local_remote_env(|mut env| {
        let rows = vec![
            NoirData::new(
                [
                    NoirType::from(0),
                    NoirType::from(8),
                    NoirType::from(f32::NAN),
                    NoirType::from(4),
                ]
                .to_vec(),
            ),
            NoirData::new(
                [
                    NoirType::from(1),
                    NoirType::from(4),
                    NoirType::from(f32::NAN),
                    NoirType::from(4),
                ]
                .to_vec(),
            ),
            NoirData::new(
                [
                    NoirType::from(f32::NAN),
                    NoirType::from(1),
                    NoirType::from(f32::NAN),
                    NoirType::from(9),
                ]
                .to_vec(),
            ),
            NoirData::new(
                [
                    NoirType::from(2),
                    NoirType::from(1),
                    NoirType::from(f32::NAN),
                    NoirType::from(3),
                ]
                .to_vec(),
            ),
        ];
        let source = IteratorSource::new(rows.into_iter());
        let res = env.stream(source).entropy(true).collect_vec();
        env.execute_blocking();

        if let Some(res) = res.get() {
            assert_eq!(
                res[0],
                NoirData::Row(vec![
                    NoirType::Float32(1.5849626),
                    NoirType::Float32(1.5),
                    NoirType::None(),
                    NoirType::Float32(1.5)
                ])
            );
        }
    });
}

#[test]
fn entropy_noir_data_nan() {
    TestHelper::local_remote_env(|mut env| {
        let rows = vec![
            NoirData::new(
                [
                    NoirType::from(0),
                    NoirType::from(f32::NAN),
                    NoirType::from(f32::NAN),
                    NoirType::from(4),
                ]
                .to_vec(),
            ),
            NoirData::new(
                [
                    NoirType::from(f32::NAN),
                    NoirType::from(4),
                    NoirType::from(f32::NAN),
                    NoirType::from(4),
                ]
                .to_vec(),
            ),
        ];
        let source = IteratorSource::new(rows.into_iter());
        let res = env.stream(source).entropy(false).collect_vec();
        env.execute_blocking();

        if let Some(res) = res.get() {
            assert_eq!(
                res[0],
                NoirData::Row(vec![
                    NoirType::NaN(),
                    NoirType::NaN(),
                    NoirType::NaN(),
                    NoirType::Float32(0.0)
                ])
            );
        }
    });
}

#[test]
fn entropy_noir_type() {
    TestHelper::local_remote_env(|mut env| {
        let rows = vec![
            NoirData::NoirType(NoirType::from(0)),
            NoirData::NoirType(NoirType::from(4)),
            NoirData::NoirType(NoirType::from(f32::NAN)),
            NoirData::NoirType(NoirType::from(4)),
        ];
        let source = IteratorSource::new(rows.into_iter());
        let res = env.stream(source).entropy(true).collect_vec();
        env.execute_blocking();

        if let Some(res) = res.get() {
            assert_eq!(res[0], NoirData::NoirType(NoirType::Float32(0.9182958)));
        }
    });
}

#[test]
fn entropy_noir_type_nan() {
    TestHelper::local_remote_env(|mut env| {
        let rows = vec![
            NoirData::NoirType(NoirType::from(0)),
            NoirData::NoirType(NoirType::from(8)),
            NoirData::NoirType(NoirType::from(f32::NAN)),
            NoirData::NoirType(NoirType::from(4)),
        ];
        let source = IteratorSource::new(rows.into_iter());
        let res = env.stream(source).entropy(false).collect_vec();
        env.execute_blocking();

        if let Some(res) = res.get() {
            assert_eq!(res[0], NoirData::NoirType(NoirType::from(f32::NAN)));
        }
    });
}

#[test]
fn entropy_noir_type_none() {
    TestHelper::local_remote_env(|mut env| {
        let rows = vec![
            NoirData::NoirType(NoirType::from(f32::NAN)),
            NoirData::NoirType(NoirType::from(f32::NAN)),
            NoirData::NoirType(NoirType::from(f32::NAN)),
            NoirData::NoirType(NoirType::from(f32::NAN)),
        ];
        let source = IteratorSource::new(rows.into_iter());
        let res = env.stream(source).entropy(true).collect_vec();
        env.execute_blocking();

        if let Some(res) = res.get() {
            assert_eq!(res[0], NoirData::NoirType(NoirType::from(None::<f32>)));
        }
    });
}
