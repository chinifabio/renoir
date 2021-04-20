use itertools::Itertools;
use rstream::operator::source::IteratorSource;
use rstream::test::TestHelper;

#[test]
fn filter_map_stream() {
    TestHelper::local_remote_env(|mut env| {
        let source = IteratorSource::new(0..10u8);
        let res = env
            .stream(source)
            .filter_map(|x| if x % 2 == 1 { Some(x * 2 + 1) } else { None })
            .collect_vec();
        env.execute();
        if let Some(res) = res.get() {
            assert_eq!(res, &[3, 7, 11, 15, 19]);
        }
    });
}

#[test]
fn filter_map_keyed_stream() {
    TestHelper::local_remote_env(|mut env| {
        let source = IteratorSource::new(0..10u8);
        let res = env
            .stream(source)
            .group_by(|n| n % 2)
            .filter_map(|(_key, x)| if x < 6 { Some(x * 2 + 1) } else { None })
            .unkey()
            .map(|(_k, v)| v)
            .collect_vec();
        env.execute();
        if let Some(mut res) = res.get() {
            res.sort_unstable();
            assert_eq!(res, (0..6u8).map(|x| x * 2 + 1).collect_vec());
        }
    });
}
