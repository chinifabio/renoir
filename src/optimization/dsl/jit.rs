use cranelift::prelude::*;
use cranelift_jit::{JITBuilder, JITModule};
use cranelift_module::{Linkage, Module};

use crate::data_type::noir_type::{NoirType, NoirTypeKind};
use crate::data_type::schema::Schema;

use super::expressions::{AggregateOp, BinaryOp, Expr, UnaryOp};

pub struct JitCompiler {
    builder_context: FunctionBuilderContext,
    ctx: codegen::Context,
    module: JITModule,
}

impl Default for JitCompiler {
    fn default() -> Self {
        let mut flag_builder = settings::builder();
        flag_builder.set("use_colocated_libcalls", "false").unwrap();
        flag_builder.set("is_pic", "false").unwrap();
        let isa_builder = cranelift_native::builder().unwrap_or_else(|msg| {
            panic!("host machine is not supported: {}", msg);
        });
        let isa = isa_builder
            .finish(settings::Flags::new(flag_builder))
            .unwrap();
        let builder = JITBuilder::with_isa(isa, cranelift_module::default_libcall_names());

        let module = JITModule::new(builder);
        Self {
            builder_context: FunctionBuilderContext::new(),
            ctx: module.make_context(),
            module,
        }
    }
}

impl JitCompiler {
    pub fn compile(&mut self, input_expr: &Expr, schema: &Schema) -> Result<*const u8, String> {
        let params = schema.columns.clone();
        let the_return = schema.compute_result_type(input_expr);
        self.translate(params, the_return, input_expr)?;
        let id = self
            .module
            .declare_function("jit_function", Linkage::Export, &self.ctx.func.signature)
            .map_err(|e| {
                println!("{:?}", e);
                e.to_string()
            })?;
        self.module
            .define_function(id, &mut self.ctx)
            .map_err(|e| {
                println!("{:#?}", e);
                e.to_string()
            })?;
        self.module.clear_context(&mut self.ctx);
        self.module.finalize_definitions().unwrap();
        let code = self.module.get_finalized_function(id);
        Ok(code)
    }

    fn translate(
        &mut self,
        params: Vec<NoirTypeKind>,
        the_return: NoirTypeKind,
        expr: &Expr,
    ) -> Result<(), String> {
        let pointer_type = self.module.target_config().pointer_type();
        self.ctx
            .func
            .signature
            .params
            .push(AbiParam::new(pointer_type));
        self.ctx
            .func
            .signature
            .params
            .push(AbiParam::new(pointer_type));

        let mut builder = FunctionBuilder::new(&mut self.ctx.func, &mut self.builder_context);
        let entry_block = builder.create_block();
        builder.append_block_params_for_function_params(entry_block);
        builder.switch_to_block(entry_block);
        builder.seal_block(entry_block);

        let mut translator = ExprTranslator { params, builder };

        let (return_value, computed_return_type) = translator.translate(expr, entry_block);
        assert_eq!(computed_return_type, the_return);

        let result_address = translator.builder.block_params(entry_block)[1];
        translator
            .builder
            .ins()
            .store(MemFlags::new(), return_value, result_address, 4);

        let discriminant_value = translator
            .builder
            .ins()
            .iconst(types::I32, the_return as i64);
        translator.builder.ins().store(
            MemFlags::new().with_heap(),
            discriminant_value,
            result_address,
            0,
        );

        translator.builder.ins().return_(&[]);
        translator.builder.finalize();
        Ok(())
    }
}

struct ExprTranslator<'a> {
    params: Vec<NoirTypeKind>,
    builder: FunctionBuilder<'a>,
}

impl<'a> ExprTranslator<'a> {
    fn translate(&mut self, expr: &Expr, block: Block) -> (Value, NoirTypeKind) {
        match expr {
            Expr::NthColumn(n) => {
                let item_base_address = self.builder.block_params(block)[0];
                let item_size = self
                    .builder
                    .ins()
                    .iconst(types::I64, std::mem::size_of::<NoirType>() as i64);
                let item_offset = self.builder.ins().imul_imm(item_size, *n as i64);
                let item_address = self.builder.ins().iadd(item_base_address, item_offset);

                let item_type = match self.params[*n] {
                    NoirTypeKind::Int32 => types::I32,
                    NoirTypeKind::Float32 => types::F32,
                    NoirTypeKind::Bool => types::I8,
                    e => panic!("{} should not be a column type", e),
                };
                let item_value = self.builder.ins().load(
                    item_type,
                    MemFlags::new().with_heap(),
                    item_address,
                    4,
                );

                (item_value, self.params[*n])
            }
            Expr::Literal(v) => match v {
                NoirType::Int32(v) => (
                    self.builder.ins().iconst(types::I32, *v as i64),
                    NoirTypeKind::Int32,
                ),
                NoirType::Float32(v) => (self.builder.ins().f32const(*v), NoirTypeKind::Float32),
                NoirType::Bool(v) => (
                    self.builder.ins().iconst(types::I8, if *v { 1 } else { 0 }),
                    NoirTypeKind::Bool,
                ),
                NoirType::NaN() => (self.builder.ins().f32const(f32::NAN), NoirTypeKind::Float32),
                NoirType::None() => (self.builder.ins().f32const(f32::NAN), NoirTypeKind::Float32),
            },
            Expr::BinaryExpr { left, op, right } => {
                let (left_val, left_type) = self.translate(left, block);
                let (right_val, right_type) = self.translate(right, block);
                match op {
                    BinaryOp::Plus => {
                        assert_eq!(left_type, right_type);
                        let val = match left_type {
                            NoirTypeKind::Int32 => self.builder.ins().iadd(left_val, right_val),
                            NoirTypeKind::Float32 => self.builder.ins().fadd(left_val, right_val),
                            NoirTypeKind::Bool => self.builder.ins().iadd(left_val, right_val),
                            e => panic!("Error during jit compiling, {} is not supported", e),
                        };
                        (val, left_type)
                    }
                    BinaryOp::Minus => {
                        assert_eq!(left_type, right_type);
                        let val = match left_type {
                            NoirTypeKind::Int32 => self.builder.ins().isub(left_val, right_val),
                            NoirTypeKind::Float32 => self.builder.ins().fsub(left_val, right_val),
                            NoirTypeKind::Bool => self.builder.ins().isub(left_val, right_val),
                            e => panic!("Error during jit compiling, {} is not supported", e),
                        };
                        (val, left_type)
                    }
                    BinaryOp::Multiply => {
                        assert_eq!(left_type, right_type);
                        let val = match left_type {
                            NoirTypeKind::Int32 => self.builder.ins().imul(left_val, right_val),
                            NoirTypeKind::Float32 => self.builder.ins().fmul(left_val, right_val),
                            NoirTypeKind::Bool => self.builder.ins().imul(left_val, right_val),
                            e => panic!("Error during jit compiling, {} is not supported", e),
                        };
                        (val, left_type)
                    }
                    BinaryOp::Divide => {
                        assert_eq!(left_type, right_type);
                        let val = match left_type {
                            NoirTypeKind::Int32 => self.builder.ins().sdiv(left_val, right_val),
                            NoirTypeKind::Float32 => self.builder.ins().fdiv(left_val, right_val),
                            NoirTypeKind::Bool => self.builder.ins().sdiv(left_val, right_val),
                            e => panic!("Error during jit compiling, {} is not supported", e),
                        };
                        (val, left_type)
                    }
                    BinaryOp::Mod => {
                        assert_eq!(left_type, right_type);
                        let val = match (left_type, right_type) {
                            (NoirTypeKind::Int32, NoirTypeKind::Int32) => {
                                self.builder.ins().srem(left_val, right_val)
                            }
                            e => panic!("Error during jit compiling, cannot crate mod for {:?}", e),
                        };
                        (val, left_type)
                    }
                    BinaryOp::Eq => {
                        assert_eq!(left_type, right_type);
                        let val = match left_type {
                            NoirTypeKind::Int32 => {
                                self.builder.ins().icmp(IntCC::Equal, left_val, right_val)
                            }
                            NoirTypeKind::Float32 => {
                                self.builder.ins().fcmp(FloatCC::Equal, left_val, right_val)
                            }
                            NoirTypeKind::Bool => {
                                self.builder.ins().icmp(IntCC::Equal, left_val, right_val)
                            }
                            e => panic!("Error during jit compiling, {} is not supported", e),
                        };
                        (val, NoirTypeKind::Bool)
                    }
                    e => panic!("Error during jit compiling, {:?} is not supported yet", e),
                }
            }
            Expr::UnaryExpr { op, expr } => {
                let (val, val_type) = self.translate(expr, block);
                match op {
                    UnaryOp::Floor => {
                        let val = match val_type {
                            NoirTypeKind::Float32 => {
                                self.builder.ins().fcvt_to_sint(types::I32, val)
                            }
                            NoirTypeKind::Int32 => val,
                            e => panic!("Error during jit compiling, cannot floor {:?}", e),
                        };
                        (val, NoirTypeKind::Int32)
                    }
                    _ => todo!(),
                }
            }
            Expr::AggregateExpr { op, expr } => match op {
                AggregateOp::Sum => self.translate(expr, block),
                AggregateOp::Count => (
                    self.builder.ins().iconst(types::I32, 1),
                    NoirTypeKind::Int32,
                ),
                _ => todo!(),
            },
            Expr::Empty => panic!("Error during jit compiling, cannot compile empty expression"),
            Expr::Compiled { .. } => {
                panic!("Error during jit compiling, cannot compile compiled expression")
            }
        }
    }
}

#[cfg(test)]
pub mod test {
    use crate::{
        data_type::{
            noir_type::{NoirType, NoirTypeKind},
            schema::Schema,
        },
        optimization::dsl::{expressions::*, jit::JitCompiler},
    };

    #[test]
    fn test_jit_compiler() {
        let schema = Schema::new(vec![NoirTypeKind::Int32, NoirTypeKind::Int32]);
        let expr = col(0) + col(1);

        let mut jit_compiler = JitCompiler::default();
        let code = jit_compiler.compile(&expr, &schema);
        assert!(code.is_ok());

        let code = code.unwrap();
        let compiled_expr: extern "C" fn(*const NoirType, *mut NoirType) =
            unsafe { std::mem::transmute(code) };

        let item = [NoirType::Int32(1), NoirType::Int32(2)];
        let mut result = NoirType::None();
        compiled_expr(item.as_ptr(), &mut result);
        assert_eq!(result, NoirType::Int32(3));
    }
}
