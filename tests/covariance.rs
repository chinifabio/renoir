use noir::{
    data_type::{NoirData, NoirType},
    operator::source::IteratorSource,
};
use utils::TestHelper;

mod utils;

#[test]
fn covariance_noir_data() {
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
                    NoirType::from(3.0),
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
        let res = env.stream(source).covariance([1, 2]).collect_vec();
        env.execute_blocking();

        if let Some(res) = res.get() {
            assert_eq!(
                res,
                [NoirData::NoirType(NoirType::Float32(-2.0))]
            );
        }
    });
}

#[test]
fn covariance_noir_data_nan() {
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
        let res = env.stream(source).covariance([2, 4]).collect_vec();
        env.execute_blocking();

        if let Some(res) = res.get() {
            assert_eq!(
                res,
                [NoirData::NoirType(NoirType::NaN())]
            );
        }
    });
}

#[test]
#[should_panic]
fn covariance_noir_type() {
    TestHelper::local_remote_env(|mut env| {
        let rows = vec![
            NoirData::NoirType(NoirType::from(0.0)),
            NoirData::NoirType(NoirType::from(8.0)),
            NoirData::NoirType(NoirType::from(f32::NAN)),
            NoirData::NoirType(NoirType::from(4.0)),
        ];
        let source = IteratorSource::new(rows.into_iter());
        env.stream(source).covariance([1,2]).collect_vec();
        env.execute_blocking();
    });
}

#[test]
fn covariance_noir_type_none() {
    TestHelper::local_remote_env(|mut env| {
        let rows = vec![
            NoirData::new(
                [
                    NoirType::None(),
                    NoirType::from(4.0),
                ]
                .to_vec(),
            ),
            NoirData::new(
                [
                    NoirType::None(),
                    NoirType::from(4.0),
                ]
                .to_vec(),
            ),
        ];
        let source = IteratorSource::new(rows.into_iter());
        let res = env.stream(source).covariance([1,2]).collect_vec();
        env.execute_blocking();

        if let Some(res) = res.get() {
            assert_eq!(res, [NoirData::NoirType(NoirType::None())]);
        }
    });
}
