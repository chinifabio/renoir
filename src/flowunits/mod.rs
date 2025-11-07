use capabilities::SpecNode;

use crate::{
    operator::{ExchangeData, Operator},
    Stream,
};

pub mod capabilities;
pub(crate) mod channels;

impl<Op, Ft: 'static> Stream<Op, Ft>
where
    Op: Operator + 'static,
    Op::Out: ExchangeData,
{
    /// TODO: docs
    pub fn update_layer(
        self,
        layer: impl Into<String>,
    ) -> Stream<impl Operator<Out = Op::Out>, Ft> {
        // the layer should be updated before the new block is created
        self.ctx.lock().update_layer(layer);
        self.shuffle()
    }

    /// TODO: docs
    pub fn update_requirements(
        self,
        requirements: SpecNode,
    ) -> Stream<impl Operator<Out = Op::Out>, Ft> {
        // requirements need the new block id to be saved
        let stream = self.shuffle();
        stream
            .ctx
            .lock()
            .update_requirements(stream.block.id, requirements);
        stream
    }
}

#[cfg(test)]
mod tests {
    use std::{any::TypeId, collections::HashMap, sync::Arc};

    use crate::{
        block::{Block, Scheduling},
        network::Coord,
        prelude::s,
        scheduler::Scheduler,
        test::FakeOperator,
        Replication,
    };

    #[test]
    fn test_layers() {
        use crate::config::ConfigBuilder;

        let mut toml_path = tempfile::NamedTempFile::new().unwrap();
        let config_toml = r#"[[host]]
address = "127.0.0.1"
base_port = 21841
num_cores = 1
layer = "layer_1"
group = "group_a"
[[host]]
address = "127.0.0.1"
base_port = 31258
num_cores = 2
layer = "layer_1"
group = "group_b"
[[host]]
address = "127.0.0.1"
base_port = 31258
num_cores = 4
layer = "layer_2"
group = "group_c"
[[group_connections]]
from = ["group_a", "group_b"]
to = "group_c"
"#;
        std::io::Write::write_all(&mut toml_path, config_toml.as_bytes()).unwrap();

        let run = |config: Arc<crate::RuntimeConfig>| {
            let mut scheduler = Scheduler::new(config.clone());

            let mut block1 = Block::new(
                0,
                FakeOperator::<u64>::empty(),
                Default::default(),
                Default::default(),
                Default::default(), // Default is unlimited replication
            );
            block1.set_layer("layer_1");
            let mut block2 = Block::new(
                1,
                FakeOperator::<u64>::empty(),
                Default::default(),
                Default::default(),
                Default::default(),
            );
            block2.set_layer("layer_2");

            let remote_config = match &*config {
                crate::RuntimeConfig::Local(_) => unreachable!(),
                crate::RuntimeConfig::Remote(remote) => remote.clone(),
            };
            let block1_info = scheduler.remote_block_info(&block1, &remote_config);
            assert_eq!(block1_info.replicas(0).len(), 1);
            assert_eq!(block1_info.replicas(1).len(), 2);
            assert_eq!(block1_info.replicas(2).len(), 0);

            let block2_info = scheduler.remote_block_info(&block2, &remote_config);
            assert_eq!(block2_info.replicas(0).len(), 0);
            assert_eq!(block2_info.replicas(1).len(), 0);
            assert_eq!(block2_info.replicas(2).len(), 4);

            scheduler.schedule_block(block1);
            scheduler.schedule_block(block2);
            scheduler.connect_blocks(0, 1, TypeId::of::<u64>());

            scheduler.build_all();

            let graph = scheduler.execution_graph();

            let mut expected_graph: HashMap<
                (Coord, TypeId),
                Vec<(Coord, bool)>,
                crate::block::CoordHasherBuilder,
            > = Default::default();
            expected_graph.insert(
                (Coord::new(0, 0, 0), TypeId::of::<u64>()),
                vec![
                    (Coord::new(1, 2, 0), false),
                    (Coord::new(1, 2, 1), false),
                    (Coord::new(1, 2, 2), false),
                    (Coord::new(1, 2, 3), false),
                ],
            );
            expected_graph.insert(
                (Coord::new(0, 1, 0), TypeId::of::<u64>()),
                vec![
                    (Coord::new(1, 2, 0), false),
                    (Coord::new(1, 2, 1), false),
                    (Coord::new(1, 2, 2), false),
                    (Coord::new(1, 2, 3), false),
                ],
            );
            expected_graph.insert(
                (Coord::new(0, 1, 1), TypeId::of::<u64>()),
                vec![
                    (Coord::new(1, 2, 0), false),
                    (Coord::new(1, 2, 1), false),
                    (Coord::new(1, 2, 2), false),
                    (Coord::new(1, 2, 3), false),
                ],
            );

            assert!(!graph.is_empty());
            assert_eq!(graph.len(), expected_graph.len());
            let mut graph = graph.clone();
            for (key, value) in graph.drain() {
                let expected_value = expected_graph.remove(&key);
                assert_eq!(expected_value, Some(value));
            }
            assert!(expected_graph.is_empty());
            assert!(graph.is_empty());
        };

        let config0 = Arc::new(
            ConfigBuilder::new_remote()
                .parse_file(toml_path.path())
                .unwrap()
                .host_id(0)
                .build()
                .unwrap(),
        );

        let config1 = Arc::new(
            ConfigBuilder::new_remote()
                .parse_file(toml_path.path())
                .unwrap()
                .host_id(1)
                .build()
                .unwrap(),
        );

        let config2 = Arc::new(
            ConfigBuilder::new_remote()
                .parse_file(toml_path.path())
                .unwrap()
                .host_id(2)
                .build()
                .unwrap(),
        );

        let join0 = std::thread::Builder::new()
            .name("host0".into())
            .spawn(move || run(config0))
            .unwrap();
        let join1 = std::thread::Builder::new()
            .name("host1".into())
            .spawn(move || run(config1))
            .unwrap();
        let join2 = std::thread::Builder::new()
            .name("host2".into())
            .spawn(move || run(config2))
            .unwrap();

        join0.join().unwrap();
        join1.join().unwrap();
        join2.join().unwrap();
    }

    #[test]
    fn test_layers_replication() {
        use crate::config::ConfigBuilder;

        let run = |config: Arc<crate::RuntimeConfig>,
                   replication1: Replication,
                   replication2: Replication| {
            let mut scheduler = Scheduler::new(config.clone());

            let mut block1 = Block::new(
                0,
                FakeOperator::<u64>::empty(),
                Default::default(),
                Default::default(),
                Scheduling {
                    replication: replication1,
                },
            );
            block1.set_layer("layer_1");
            let mut block2 = Block::new(
                1,
                FakeOperator::<u64>::empty(),
                Default::default(),
                Default::default(),
                Scheduling {
                    replication: replication2,
                },
            );
            block2.set_layer("layer_2");

            let remote_config = match &*config {
                crate::RuntimeConfig::Local(_) => unreachable!(),
                crate::RuntimeConfig::Remote(remote) => remote.clone(),
            };

            let replica1 = match replication1 {
                Replication::Unlimited => config.hosts().first().unwrap().num_cores,
                Replication::One => 1,
                _ => unreachable!(),
            };

            let replica2 = match replication2 {
                Replication::Unlimited => config.hosts().iter().skip(1).next().unwrap().num_cores,
                Replication::One => 1,
                _ => unreachable!(),
            };
            let block1_info = scheduler.remote_block_info(&block1, &remote_config);
            assert_eq!(block1_info.replicas(0).len(), replica1 as usize);
            assert_eq!(block1_info.replicas(1).len(), 0);

            let block2_info = scheduler.remote_block_info(&block2, &remote_config);
            assert_eq!(block2_info.replicas(0).len(), 0);
            assert_eq!(block2_info.replicas(1).len(), replica2 as usize);

            scheduler.schedule_block(block1);
            scheduler.schedule_block(block2);
            scheduler.connect_blocks(0, 1, TypeId::of::<u64>());

            scheduler.build_all();

            let graph = scheduler.execution_graph();

            let mut expected_graph: HashMap<
                (Coord, TypeId),
                Vec<(Coord, bool)>,
                crate::block::CoordHasherBuilder,
            > = Default::default();

            for r in 0..replica1 {
                expected_graph.insert(
                    (Coord::new(0, 0, r), TypeId::of::<u64>()),
                    (0..replica2)
                        .map(|rr| (Coord::new(1, 1, rr), false))
                        .collect(),
                );
            }

            assert!(!graph.is_empty());
            assert_eq!(graph.len(), expected_graph.len());
            let mut graph = graph.clone();
            for (key, value) in graph.drain() {
                let expected_value = expected_graph.remove(&key);
                assert_eq!(expected_value, Some(value));
            }
            assert!(expected_graph.is_empty());
            assert!(graph.is_empty());
        };

        let n = 4;
        for core1 in [1, n] {
            for core2 in [1, n] {
                for replication1 in [Replication::One, Replication::Unlimited] {
                    for replication2 in [Replication::One, Replication::Unlimited] {
                        let config_toml = format!(
                            r#"[[host]]
address = "127.0.0.1"
base_port = 21841
num_cores = {core1}
layer = "layer_1"
group = "group_a"
[[host]]
address = "127.0.0.1"
base_port = 31258
num_cores = {core2}
layer = "layer_2"
group = "group_b"
[[group_connections]]
from = ["group_a"]
to = "group_b"
"#
                        );
                        let mut toml_path = tempfile::NamedTempFile::new().unwrap();
                        std::io::Write::write_all(&mut toml_path, config_toml.as_bytes()).unwrap();
                        let config0 = Arc::new(
                            ConfigBuilder::new_remote()
                                .parse_file(toml_path.path())
                                .unwrap()
                                .host_id(0)
                                .build()
                                .unwrap(),
                        );

                        let config1 = Arc::new(
                            ConfigBuilder::new_remote()
                                .parse_file(toml_path.path())
                                .unwrap()
                                .host_id(1)
                                .build()
                                .unwrap(),
                        );

                        let join0 = std::thread::Builder::new()
                            .name("host0".into())
                            .spawn(move || run(config0, replication1, replication2))
                            .unwrap();
                        let join1 = std::thread::Builder::new()
                            .name("host1".into())
                            .spawn(move || run(config1, replication1, replication2))
                            .unwrap();

                        join0.join().unwrap();
                        join1.join().unwrap();
                    }
                }
            }
        }
    }

    #[test]
    fn test_requirements() {
        use crate::config::ConfigBuilder;

        let mut toml_path = tempfile::NamedTempFile::new().unwrap();
        let config_toml = r#"[[host]]
address = "127.0.0.1"
base_port = 21841
num_cores = 1
[host.capabilities]
a = "yes"
b = 1
[[host]]
address = "127.0.0.1"
base_port = 31258
num_cores = 2
[host.capabilities]
a = "yes"
b = 2
[[host]]
address = "127.0.0.1"
base_port = 31258
num_cores = 4
[host.capabilities]
a = "no"
b = 3
"#;
        std::io::Write::write_all(&mut toml_path, config_toml.as_bytes()).unwrap();

        let run = |config: Arc<crate::RuntimeConfig>| {
            let mut scheduler = Scheduler::new(config.clone());

            let block1 = Block::new(
                0,
                FakeOperator::<u64>::empty(),
                Default::default(),
                Default::default(),
                Default::default(), // Default is unlimited replication
            );
            scheduler.update_requirements(0, s("a").eq("yes"));
            let block2 = Block::new(
                1,
                FakeOperator::<u64>::empty(),
                Default::default(),
                Default::default(),
                Default::default(),
            );
            scheduler.update_requirements(1, s("b").gt(1));

            let remote_config = match &*config {
                crate::RuntimeConfig::Local(_) => unreachable!(),
                crate::RuntimeConfig::Remote(remote) => remote.clone(),
            };
            let block1_info = scheduler.remote_block_info(&block1, &remote_config);
            assert_eq!(block1_info.replicas(0).len(), 1);
            assert_eq!(block1_info.replicas(1).len(), 2);
            assert_eq!(block1_info.replicas(2).len(), 0);

            let block2_info = scheduler.remote_block_info(&block2, &remote_config);
            assert_eq!(block2_info.replicas(0).len(), 0);
            assert_eq!(block2_info.replicas(1).len(), 2);
            assert_eq!(block2_info.replicas(2).len(), 4);
        };

        let config0 = Arc::new(
            ConfigBuilder::new_remote()
                .parse_file(toml_path.path())
                .unwrap()
                .host_id(0)
                .build()
                .unwrap(),
        );

        let config1 = Arc::new(
            ConfigBuilder::new_remote()
                .parse_file(toml_path.path())
                .unwrap()
                .host_id(1)
                .build()
                .unwrap(),
        );

        let config2 = Arc::new(
            ConfigBuilder::new_remote()
                .parse_file(toml_path.path())
                .unwrap()
                .host_id(2)
                .build()
                .unwrap(),
        );

        let join0 = std::thread::Builder::new()
            .name("host0".into())
            .spawn(move || run(config0))
            .unwrap();
        let join1 = std::thread::Builder::new()
            .name("host1".into())
            .spawn(move || run(config1))
            .unwrap();
        let join2 = std::thread::Builder::new()
            .name("host2".into())
            .spawn(move || run(config2))
            .unwrap();

        join0.join().unwrap();
        join1.join().unwrap();
        join2.join().unwrap();
    }
}
