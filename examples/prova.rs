use std::env;

use noir_compute::prelude::*;

fn main() {
    env_logger::init();
    let source_file = env::args().nth(1).expect("dammi un file");

    let mut env = StreamEnvironment::default();
    let res = env
        .stream_csv_optimized(source_file)
        .group_by(vec![col(0) % 5])
        .select([avg(col(1) + col(2))])
        .filter(col(0).gte(50))
        .collect_vec();
    env.execute_blocking();
    match res.get() {
        Some(data) => {
            for i in data.iter() {
                println!("data: {}", i);
            }
        }
        None => println!("Data not present"),
    }
}
