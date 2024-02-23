use noir_compute::{data_type::{noir_type::{NoirType, NoirTypeKind}, schema::Schema, stream_item::StreamItem}, optimization::dsl::jit::JIT, prelude::*};

trait TupleAppend<T> {
    type ResultType;

    fn append(self, t: T) -> Self::ResultType;
}

impl<T> TupleAppend<T> for () {
    type ResultType = (T,);

    fn append(self, t: T) -> Self::ResultType {
        (t,)
    }
}

macro_rules! impl_tuple_append {
    ( () ) => {};
    ( ( $t0:ident $(, $types:ident)* ) ) => {
        impl<$t0, $($types,)* T> TupleAppend<T> for ($t0, $($types,)*) {
            // Trailing comma, just to be extra sure we are dealing
            // with a tuple and not a parenthesized type/expr.
            type ResultType = ($t0, $($types,)* T,);

            fn append(self, t: T) -> Self::ResultType {
                // Reuse the type identifiers to destructure ourselves:
                let ($t0, $($types,)*) = self;
                // Create a new tuple with the original elements, plus the new one:
                ($t0, $($types,)* t,)
            }
        }

        // Recurse for one smaller size:
        impl_tuple_append! { ($($types),*) }
    };
}

impl_tuple_append! {
    // Supports tuples up to size 10:
    (_1, _2, _3, _4, _5, _6, _7, _8, _9, _10)
}

fn main() {
    let expr = col(0) + 1;
    let mut jit = JIT::default();
    let schema = Schema::same_type(1, NoirTypeKind::Int32);
    let code_ptr = jit.compile(expr, schema);

    let stream_item = StreamItem::new(vec![NoirType::Int32(1)]);
    let mut input = ();
    for (i, elem) in stream_item.get_value().into_iter().enumerate() {
        let x = match elem {
            NoirType::Int32(x) => *x,
            _ => panic!("Invalid type"),
        };
        input.append(x);
    }

    // unsafe {
    //     let res = match expr.evaluate(&schema) {
    //         NoirType::Int32(_) => run_code::<i32>(code_ptr.unwrap(), NoirType::Int32(1)),
    //         NoirType::Float32(_) => todo!(),
    //         NoirType::Bool(_) => todo!(),
    //         _ => todo!(),
    //     };
    // }
}

unsafe fn run_code<I, O: Into<NoirType>>(code_ptr: *const u8, input: I) -> Result<O, String> {
    let code_fn = std::mem::transmute::<_, fn(I) -> O>(code_ptr);
    Ok(code_fn(input))
}