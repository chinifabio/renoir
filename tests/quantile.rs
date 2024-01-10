use noir_compute::{
    data_type::{NoirData, NoirType},
    prelude::IteratorSource,
};
use utils::TestHelper;

mod utils;

#[test]
fn quantiles_noir_type() {
    TestHelper::local_remote_env(|mut env| {
        let mut data = Vec::new();

        for i in 1..1001 {
            data.push(NoirData::NoirType(NoirType::Int32(i)));
        }

        let source = IteratorSource::new(data.into_iter());
        let res = env
            .stream(source)
            .quantile_parallel(0.7, true)
            .collect_vec();
        env.execute_blocking();

        if let Some(res) = res.get() {
            assert_eq!(res, [NoirData::NoirType(NoirType::from(700.5))]);
        }
    });
}

#[test]
fn quantile_noir_data() {
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
        let res = env
            .stream(source)
            .quantile_parallel(0.5, true)
            .collect_vec();
        env.execute_blocking();

        if let Some(res) = res.get() {
            assert_eq!(
                res,
                [NoirData::Row(vec![
                    NoirType::from(1.0),
                    NoirType::from(6.0),
                    NoirType::from(None::<f32>),
                    NoirType::from(4.0)
                ])]
            );
        }
    });
}

#[test]
fn quantile_noir_data_nan() {
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
        let res = env
            .stream(source)
            .quantile_parallel(0.5, false)
            .collect_vec();
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
fn quantile_noir_type() {
    TestHelper::local_remote_env(|mut env| {
        let rows = vec![
            NoirData::NoirType(NoirType::from(0.0)),
            NoirData::NoirType(NoirType::from(8.0)),
            NoirData::NoirType(NoirType::from(f32::NAN)),
            NoirData::NoirType(NoirType::from(4.0)),
        ];
        let source = IteratorSource::new(rows.into_iter());
        let res = env
            .stream(source)
            .quantile_parallel(0.5, true)
            .collect_vec();
        env.execute_blocking();

        if let Some(res) = res.get() {
            assert_eq!(res, [NoirData::NoirType(NoirType::from(4.0))]);
        }
    });
}

#[test]
fn quantile_noir_type_nan() {
    TestHelper::local_remote_env(|mut env| {
        let rows = vec![
            NoirData::NoirType(NoirType::from(0.0)),
            NoirData::NoirType(NoirType::from(8.0)),
            NoirData::NoirType(NoirType::from(f32::NAN)),
            NoirData::NoirType(NoirType::from(4.0)),
        ];
        let source = IteratorSource::new(rows.into_iter());
        let res = env
            .stream(source)
            .quantile_parallel(0.5, false)
            .collect_vec();
        env.execute_blocking();

        if let Some(res) = res.get() {
            assert_eq!(res, [NoirData::NoirType(NoirType::from(f32::NAN))]);
        }
    });
}

#[test]
fn quantile_noir_type_none() {
    TestHelper::local_remote_env(|mut env| {
        let rows = vec![
            NoirData::NoirType(NoirType::from(f32::NAN)),
            NoirData::NoirType(NoirType::from(f32::NAN)),
            NoirData::NoirType(NoirType::from(f32::NAN)),
            NoirData::NoirType(NoirType::from(f32::NAN)),
        ];
        let source = IteratorSource::new(rows.into_iter());
        let res = env
            .stream(source)
            .quantile_parallel(0.5, true)
            .collect_vec();
        env.execute_blocking();

        if let Some(res) = res.get() {
            assert_eq!(res, [NoirData::NoirType(NoirType::from(None::<f32>))]);
        }
    });
}