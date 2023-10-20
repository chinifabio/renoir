use noir::{
    data_type::{NoirData, NoirType},
    operator::source::IteratorSource,
};
use utils::TestHelper;

mod utils;

#[test]
fn drop_row() {
    TestHelper::local_remote_env(|mut env| {
        let rows = vec![
            NoirData::new(
                [
                    NoirType::from(0.0),
                    NoirType::from(8.0),
                    NoirType::from(Option::<f32>::None),
                    NoirType::from(4.0),
                ]
                .to_vec(),
            ),
            NoirData::new(
                [
                    NoirType::from(1.0),
                    NoirType::from(4.0),
                    NoirType::from(1.0),
                    NoirType::from(4.0),
                ]
                .to_vec(),
            ),
            NoirData::new(
                [
                    NoirType::from(Option::<f32>::None),
                    NoirType::from(1.0),
                    NoirType::from(Option::<f32>::None),
                    NoirType::from(9.0),
                ]
                .to_vec(),
            ),
            NoirData::new(
                [
                    NoirType::from(2.0),
                    NoirType::from(9.0),
                    NoirType::from(1.0),
                    NoirType::from(3.0),
                ]
                .to_vec(),
            ),
        ];
        let source = IteratorSource::new(rows.into_iter());
        let res = env.stream(source).drop_none().collect_vec();
        env.execute_blocking();

        if let Some(res) = res.get() {
            assert_eq!(
                res,
                [
                    NoirData::new(
                        [
                            NoirType::from(1.0),
                            NoirType::from(4.0),
                            NoirType::from(1.0),
                            NoirType::from(4.0),
                        ]
                        .to_vec(),
                    ),
                    NoirData::new(
                        [
                            NoirType::from(2.0),
                            NoirType::from(9.0),
                            NoirType::from(1.0),
                            NoirType::from(3.0),
                        ]
                        .to_vec(),
                    )
                ]
            );
        }
    });
}

#[test]
fn drop_columns() {
    TestHelper::local_remote_env(|mut env| {
        let rows = vec![
            NoirData::new(
                [
                    NoirType::from(0.0),
                    NoirType::from(8.0),
                    NoirType::from(Option::<f32>::None),
                    NoirType::from(4.0),
                ]
                .to_vec(),
            ),
            NoirData::new(
                [
                    NoirType::from(1.0),
                    NoirType::from(4.0),
                    NoirType::from(1.0),
                    NoirType::from(4.0),
                ]
                .to_vec(),
            ),
            NoirData::new(
                [
                    NoirType::from(Option::<f32>::None),
                    NoirType::from(1.0),
                    NoirType::from(Option::<f32>::None),
                    NoirType::from(9.0),
                ]
                .to_vec(),
            ),
            NoirData::new(
                [
                    NoirType::from(2.0),
                    NoirType::from(9.0),
                    NoirType::from(1.0),
                    NoirType::from(3.0),
                ]
                .to_vec(),
            ),
        ];
        let source = IteratorSource::new(rows.into_iter());
        let res = env.stream(source).drop_columns(vec![1, 3]).collect_vec();
        env.execute_blocking();

        if let Some(res) = res.get() {
            assert_eq!(
                res,
                vec![
                    NoirData::new([NoirType::from(8.0), NoirType::from(4.0),].to_vec(),),
                    NoirData::new([NoirType::from(4.0), NoirType::from(4.0),].to_vec(),),
                    NoirData::new([NoirType::from(1.0), NoirType::from(9.0),].to_vec(),),
                    NoirData::new([NoirType::from(9.0), NoirType::from(3.0),].to_vec(),),
                ]
            );
        }
    });
}

#[test]
fn fill_constant() {
    TestHelper::local_remote_env(|mut env| {
        let rows = vec![
            NoirData::new(
                [
                    NoirType::from(0.0),
                    NoirType::from(8.0),
                    NoirType::from(Option::<f32>::None),
                    NoirType::from(4.0),
                ]
                .to_vec(),
            ),
            NoirData::new(
                [
                    NoirType::from(1.0),
                    NoirType::from(4.0),
                    NoirType::from(1.0),
                    NoirType::from(4.0),
                ]
                .to_vec(),
            ),
            NoirData::new(
                [
                    NoirType::from(Option::<f32>::None),
                    NoirType::from(1.0),
                    NoirType::from(Option::<f32>::None),
                    NoirType::from(9.0),
                ]
                .to_vec(),
            ),
            NoirData::new(
                [
                    NoirType::from(2.0),
                    NoirType::from(9.0),
                    NoirType::from(1.0),
                    NoirType::from(3.0),
                ]
                .to_vec(),
            ),
        ];
        let source = IteratorSource::new(rows.into_iter());
        let res = env
            .stream(source)
            .fill_constant(NoirType::Float32(3.0))
            .collect_vec();
        env.execute_blocking();

        if let Some(res) = res.get() {
            assert_eq!(
                res,
                vec![
                    NoirData::new(
                        [
                            NoirType::from(0.0),
                            NoirType::from(8.0),
                            NoirType::from(3.0),
                            NoirType::from(4.0),
                        ]
                        .to_vec(),
                    ),
                    NoirData::new(
                        [
                            NoirType::from(1.0),
                            NoirType::from(4.0),
                            NoirType::from(1.0),
                            NoirType::from(4.0),
                        ]
                        .to_vec(),
                    ),
                    NoirData::new(
                        [
                            NoirType::from(3.0),
                            NoirType::from(1.0),
                            NoirType::from(3.0),
                            NoirType::from(9.0),
                        ]
                        .to_vec(),
                    ),
                    NoirData::new(
                        [
                            NoirType::from(2.0),
                            NoirType::from(9.0),
                            NoirType::from(1.0),
                            NoirType::from(3.0),
                        ]
                        .to_vec(),
                    ),
                ]
            );
        }
    });
}

#[test]
fn fill_back() {
    TestHelper::local_remote_env(|mut env| {
        let rows = vec![
            NoirData::new(
                [
                    NoirType::from(0.0),
                    NoirType::from(8.0),
                    NoirType::from(Option::<f32>::None),
                    NoirType::from(4.0),
                ]
                .to_vec(),
            ),
            NoirData::new(
                [
                    NoirType::from(1.0),
                    NoirType::from(4.0),
                    NoirType::from(1.0),
                    NoirType::from(4.0),
                ]
                .to_vec(),
            ),
            NoirData::new(
                [
                    NoirType::from(Option::<f32>::None),
                    NoirType::from(1.0),
                    NoirType::from(Option::<f32>::None),
                    NoirType::from(9.0),
                ]
                .to_vec(),
            ),
            NoirData::new(
                [
                    NoirType::from(2.0),
                    NoirType::from(9.0),
                    NoirType::from(1.0),
                    NoirType::from(3.0),
                ]
                .to_vec(),
            ),
        ];
        let source = IteratorSource::new(rows.into_iter());
        let res = env.stream(source).fill_backward().collect_vec();
        env.execute_blocking();

        if let Some(res) = res.get() {
            assert_eq!(
                res,
                vec![
                    NoirData::new(
                        [
                            NoirType::from(0.0),
                            NoirType::from(8.0),
                            NoirType::None(),
                            NoirType::from(4.0),
                        ]
                        .to_vec(),
                    ),
                    NoirData::new(
                        [
                            NoirType::from(1.0),
                            NoirType::from(4.0),
                            NoirType::from(1.0),
                            NoirType::from(4.0),
                        ]
                        .to_vec(),
                    ),
                    NoirData::new(
                        [
                            NoirType::from(1.0),
                            NoirType::from(1.0),
                            NoirType::from(1.0),
                            NoirType::from(9.0),
                        ]
                        .to_vec(),
                    ),
                    NoirData::new(
                        [
                            NoirType::from(2.0),
                            NoirType::from(9.0),
                            NoirType::from(1.0),
                            NoirType::from(3.0),
                        ]
                        .to_vec(),
                    ),
                ]
            );
        }
    });
}

#[test]
fn fill_max() {
    TestHelper::local_remote_env(|mut env| {
        let rows = vec![
            NoirData::new(
                [
                    NoirType::from(0.0),
                    NoirType::from(8.0),
                    NoirType::from(Option::<f32>::None),
                    NoirType::from(4.0),
                ]
                .to_vec(),
            ),
            NoirData::new(
                [
                    NoirType::from(1.0),
                    NoirType::from(4.0),
                    NoirType::from(1.0),
                    NoirType::from(4.0),
                ]
                .to_vec(),
            ),
            NoirData::new(
                [
                    NoirType::from(Option::<f32>::None),
                    NoirType::from(1.0),
                    NoirType::from(Option::<f32>::None),
                    NoirType::from(9.0),
                ]
                .to_vec(),
            ),
            NoirData::new(
                [
                    NoirType::from(2.0),
                    NoirType::from(9.0),
                    NoirType::from(1.0),
                    NoirType::from(3.0),
                ]
                .to_vec(),
            ),
        ];
        let source = IteratorSource::new(rows.into_iter());
        let res = env.stream(source).fill_max().collect_vec();
        env.execute_blocking();

        if let Some(res) = res.get() {
            assert!(res.iter().all(|v| !v.contains_none()));
        }
    });
}
