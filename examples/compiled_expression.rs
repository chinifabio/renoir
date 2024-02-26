use noir_compute::{
    data_type::{
        noir_type::{NoirType, NoirTypeKind},
        schema::Schema,
        stream_item::StreamItem,
    },
    optimization::dsl::jit::JitCompiler,
    prelude::*,
};
use rand::{rngs::ThreadRng, Rng};

fn random_row(rng: &mut ThreadRng) -> StreamItem {
    let col = 5;
    let mut row = Vec::with_capacity(col);
    for _ in 0..col {
        row.push(NoirType::Int32(rng.gen_range(0..100)))
    }
    StreamItem::new(row)
}

fn random_vec(rng: &mut ThreadRng) -> Vec<NoirType> {
    let col = 5;
    let mut row = Vec::with_capacity(col);
    for _ in 0..col {
        row.push(NoirType::Int32(rng.gen_range(0..100)))
    }
    row
}

fn main() {
    let mut rng = rand::thread_rng();

    let size = 1000000;
    let mut data = Vec::with_capacity(size);
    for _ in 0..size {
        data.push(random_vec(&mut rng))
    }

    let expr = col(3).modulo(7).eq(0);
    let time = std::time::Instant::now();
    for row in &data {
        expr.evaluate(row);
    }
    println!("Time taken: {:?}", time.elapsed());
    let schema = Schema::same_type(5, NoirTypeKind::Int32);
    let compiled_expr = CompiledExpr::compile(expr, schema, &mut JitCompiler::default());
    let time = std::time::Instant::now();
    for row in &data {
        compiled_expr.evaluate(row);
    }
    println!("Time taken: {:?}", time.elapsed());
}
