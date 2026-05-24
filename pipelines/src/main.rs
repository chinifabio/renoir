#![allow(non_camel_case_types)]
#![allow(unused_variables)]
#![allow(non_snake_case)]
use renoir::{config::ConfigBuilder, prelude::*};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize, PartialOrd, PartialEq, Default, Eq, Hash)]
struct Struct_users {
    id: Option<i64>,

    name: Option<String>,

    age: Option<i64>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialOrd, PartialEq, Default, Eq, Hash)]
struct Struct_stream0 {
    id_users: Option<i64>,

    name_users: Option<String>,

    age_users: Option<i64>,
}

fn main() {
    {
        let config = ConfigBuilder::new_local(1).unwrap();

        let ctx = StreamContext::new(config.clone());
        let stream0 = ctx
            .stream_csv::<Struct_users>("/tmp/.tmpTZijW9")
            .filter(move |x| {
                if x.age.is_some() {
                    x.age.unwrap() > 25
                } else {
                    false
                }
            })
            .map(|x| Struct_stream0 {
                id_users: x.id,
                name_users: x.name,
                age_users: x.age,
            })
            .write_csv(move |_| r"pipelines/output.csv".into(), true);

        ctx.execute_blocking();
    }
}
