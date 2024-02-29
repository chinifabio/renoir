use cranelift::prelude::*;
use cranelift_jit::{JITBuilder, JITModule};
use cranelift_module::{Linkage, Module};

use crate::data_type::noir_type::{NoirType, NoirTypeKind};
use crate::data_type::schema::Schema;

use super::expressions::{BinaryOp, Expr, UnaryOp};

pub struct JitCompiler {
    builder_context: FunctionBuilderContext,
    ctx: codegen::Context,
    module: JITModule,
    index: usize,
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
            index: 0,
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
            .declare_function(
                format!("compiled_expression_{}", self.index).as_str(),
                Linkage::Export,
                &self.ctx.func.signature,
            )
            .map_err(|e| {
                println!("{:?}", e);
                e.to_string()
            })?;
        self.index += 1;
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
            .returns
            .push(AbiParam::new(types::I64));

        let mut builder = FunctionBuilder::new(&mut self.ctx.func, &mut self.builder_context);
        let entry_block = builder.create_block();
        builder.append_block_params_for_function_params(entry_block);
        builder.switch_to_block(entry_block);
        builder.seal_block(entry_block);

        let mut translator = ExprTranslator { params, builder };

        let (result_value, computed_return_type) = translator.translate(expr, entry_block);
        assert_eq!(computed_return_type, the_return);

        let discriminant_value = translator
            .builder
            .ins()
            .iconst(types::I32, the_return as i64);

        // let return_value = translator.builder.ins().iconcat(discriminant_value, result_value);
        let mut return_value = translator.builder.ins().uextend(types::I64, result_value);
        return_value = translator.builder.ins().ishl_imm(return_value, 32);
        let other = translator
            .builder
            .ins()
            .uextend(types::I64, discriminant_value);
        return_value = translator.builder.ins().bor(return_value, other);

        translator.builder.ins().return_(&[return_value]);
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
                let item_offset = std::mem::size_of::<NoirType>() as i64 * *n as i64;
                let item_address = self.builder.ins().iadd_imm(item_base_address, item_offset);

                let item_type = match self.params[*n] {
                    NoirTypeKind::Int32 => types::I32,
                    NoirTypeKind::Float32 => types::F32,
                    NoirTypeKind::Bool => types::I32,
                    e => panic!("{} should not be a column type", e),
                };
                let item_value =
                    self.builder
                        .ins()
                        .load(item_type, MemFlags::new(), item_address, 4);

                (item_value, self.params[*n])
            }
            Expr::Literal(v) => match v {
                NoirType::Int32(v) => (
                    self.builder.ins().iconst(types::I32, *v as i64),
                    NoirTypeKind::Int32,
                ),
                NoirType::Float32(v) => (self.builder.ins().f32const(*v), NoirTypeKind::Float32),
                NoirType::Bool(v) => (
                    self.builder
                        .ins()
                        .iconst(types::I32, if *v { 1 } else { 0 }),
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
                        let (left_val, right_val, values_type) = self
                            .ensure_compatible_types(left_val, right_val, left_type, right_type);
                        let val = match values_type {
                            NoirTypeKind::Int32 => self.builder.ins().iadd(left_val, right_val),
                            NoirTypeKind::Float32 => self.builder.ins().fadd(left_val, right_val),
                            NoirTypeKind::Bool => self.builder.ins().iadd(left_val, right_val),
                            e => panic!("Error during jit compiling, {} is not supported", e),
                        };
                        (val, values_type)
                    }
                    BinaryOp::Minus => {
                        let (left_val, right_val, values_type) = self
                            .ensure_compatible_types(left_val, right_val, left_type, right_type);
                        let val = match values_type {
                            NoirTypeKind::Int32 => self.builder.ins().isub(left_val, right_val),
                            NoirTypeKind::Float32 => self.builder.ins().fsub(left_val, right_val),
                            NoirTypeKind::Bool => self.builder.ins().isub(left_val, right_val),
                            e => panic!("Error during jit compiling, {} is not supported", e),
                        };
                        (val, values_type)
                    }
                    BinaryOp::Multiply => {
                        let (left_val, right_val, values_type) = self
                            .ensure_compatible_types(left_val, right_val, left_type, right_type);
                        let val = match values_type {
                            NoirTypeKind::Int32 => self.builder.ins().imul(left_val, right_val),
                            NoirTypeKind::Float32 => self.builder.ins().fmul(left_val, right_val),
                            NoirTypeKind::Bool => self.builder.ins().imul(left_val, right_val),
                            e => panic!("Error during jit compiling, {} is not supported", e),
                        };
                        (val, values_type)
                    }
                    BinaryOp::Divide => {
                        let (left_val, right_val, values_type) = self
                            .ensure_compatible_types(left_val, right_val, left_type, right_type);
                        let val = match values_type {
                            NoirTypeKind::Int32 => self.builder.ins().sdiv(left_val, right_val),
                            NoirTypeKind::Float32 => self.builder.ins().fdiv(left_val, right_val),
                            NoirTypeKind::Bool => self.builder.ins().sdiv(left_val, right_val),
                            e => panic!("Error during jit compiling, {} is not supported", e),
                        };
                        (val, values_type)
                    }
                    BinaryOp::Mod => {
                        let val = match (left_type, right_type) {
                            (NoirTypeKind::Int32, NoirTypeKind::Int32) => {
                                self.builder.ins().srem(left_val, right_val)
                            }
                            (NoirTypeKind::Float32, NoirTypeKind::Int32) => {
                                let left_val =
                                    self.builder.ins().fcvt_to_sint(types::F32, left_val);
                                self.builder.ins().srem(left_val, right_val)
                            }
                            e => panic!("Error during jit compiling, cannot crate mod for {:?}", e),
                        };
                        (val, left_type)
                    }
                    BinaryOp::Eq => {
                        let (left_val, right_val, values_type) = self
                            .ensure_compatible_types(left_val, right_val, left_type, right_type);
                        let val = match values_type {
                            NoirTypeKind::Int32 => {
                                self.handle_int_comparison(left_val, right_val, IntCC::Equal)
                            }
                            NoirTypeKind::Float32 => {
                                self.handle_float_comparison(left_val, right_val, FloatCC::Equal)
                            }
                            NoirTypeKind::Bool => {
                                self.handle_int_comparison(left_val, right_val, IntCC::Equal)
                            }
                            e => panic!("Error during jit compiling, cannot compare {:?}", e),
                        };
                        (val, NoirTypeKind::Bool)
                    }
                    BinaryOp::NotEq => {
                        let (left_val, right_val, values_type) = self
                            .ensure_compatible_types(left_val, right_val, left_type, right_type);
                        let val = match values_type {
                            NoirTypeKind::Int32 => {
                                self.handle_int_comparison(left_val, right_val, IntCC::NotEqual)
                            }
                            NoirTypeKind::Float32 => {
                                self.handle_float_comparison(left_val, right_val, FloatCC::NotEqual)
                            }
                            NoirTypeKind::Bool => {
                                self.handle_int_comparison(left_val, right_val, IntCC::NotEqual)
                            }
                            e => panic!("Error during jit compiling, cannot compare {:?}", e),
                        };
                        (val, NoirTypeKind::Bool)
                    }
                    BinaryOp::Gt => {
                        let (left_val, right_val, values_type) = self
                            .ensure_compatible_types(left_val, right_val, left_type, right_type);
                        let val = match values_type {
                            NoirTypeKind::Int32 => self.handle_int_comparison(
                                left_val,
                                right_val,
                                IntCC::SignedGreaterThan,
                            ),
                            NoirTypeKind::Float32 => self.handle_float_comparison(
                                left_val,
                                right_val,
                                FloatCC::GreaterThan,
                            ),
                            NoirTypeKind::Bool => self.handle_int_comparison(
                                left_val,
                                right_val,
                                IntCC::SignedGreaterThan,
                            ),
                            e => panic!("Error during jit compiling, cannot compare {:?}", e),
                        };
                        (val, NoirTypeKind::Bool)
                    }
                    BinaryOp::GtEq => {
                        let (left_val, right_val, values_type) = self
                            .ensure_compatible_types(left_val, right_val, left_type, right_type);
                        let val = match values_type {
                            NoirTypeKind::Int32 => self.handle_int_comparison(
                                left_val,
                                right_val,
                                IntCC::SignedGreaterThanOrEqual,
                            ),
                            NoirTypeKind::Float32 => self.handle_float_comparison(
                                left_val,
                                right_val,
                                FloatCC::GreaterThanOrEqual,
                            ),
                            NoirTypeKind::Bool => self.handle_int_comparison(
                                left_val,
                                right_val,
                                IntCC::SignedGreaterThanOrEqual,
                            ),
                            e => panic!("Error during jit compiling, cannot compare {:?}", e),
                        };
                        (val, NoirTypeKind::Bool)
                    }
                    BinaryOp::Lt => {
                        let (left_val, right_val, values_type) = self
                            .ensure_compatible_types(left_val, right_val, left_type, right_type);
                        let val = match values_type {
                            NoirTypeKind::Int32 => self.handle_int_comparison(
                                left_val,
                                right_val,
                                IntCC::SignedLessThan,
                            ),
                            NoirTypeKind::Float32 => {
                                self.handle_float_comparison(left_val, right_val, FloatCC::LessThan)
                            }
                            NoirTypeKind::Bool => self.handle_int_comparison(
                                left_val,
                                right_val,
                                IntCC::SignedLessThan,
                            ),
                            e => panic!("Error during jit compiling, cannot compare {:?}", e),
                        };
                        (val, NoirTypeKind::Bool)
                    }
                    BinaryOp::LtEq => {
                        let (left_val, right_val, values_type) = self
                            .ensure_compatible_types(left_val, right_val, left_type, right_type);
                        let val = match values_type {
                            NoirTypeKind::Int32 => self.handle_int_comparison(
                                left_val,
                                right_val,
                                IntCC::SignedLessThanOrEqual,
                            ),
                            NoirTypeKind::Float32 => self.handle_float_comparison(
                                left_val,
                                right_val,
                                FloatCC::LessThanOrEqual,
                            ),
                            NoirTypeKind::Bool => self.handle_int_comparison(
                                left_val,
                                right_val,
                                IntCC::SignedLessThanOrEqual,
                            ),
                            e => panic!("Error during jit compiling, cannot compare {:?}", e),
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
            Expr::AggregateExpr { .. } => panic!("Error during jit compiling, cannot aggregate"),
            Expr::Empty => panic!("Error during jit compiling, cannot compile empty expression"),
            Expr::Compiled { .. } => {
                panic!("Error during jit compiling, cannot compile compiled expression")
            }
        }
    }

    fn ensure_compatible_types(
        &mut self,
        left_val: Value,
        right_val: Value,
        left_type: NoirTypeKind,
        right_type: NoirTypeKind,
    ) -> (Value, Value, NoirTypeKind) {
        match (left_type, right_type) {
            (NoirTypeKind::Int32, NoirTypeKind::Int32) => (left_val, right_val, left_type),
            (NoirTypeKind::Float32, NoirTypeKind::Float32) => (left_val, right_val, left_type),
            (NoirTypeKind::Bool, NoirTypeKind::Bool) => (left_val, right_val, left_type),
            (NoirTypeKind::Int32, NoirTypeKind::Float32) => {
                let left_val = self.builder.ins().fcvt_to_sint(types::F32, left_val);
                (left_val, right_val, NoirTypeKind::Float32)
            }
            (NoirTypeKind::Int32, NoirTypeKind::Bool) => (left_val, right_val, NoirTypeKind::Int32),
            (NoirTypeKind::Float32, NoirTypeKind::Int32) => {
                let right_val = self.builder.ins().fcvt_to_sint(types::F32, right_val);
                (left_val, right_val, NoirTypeKind::Float32)
            }
            (NoirTypeKind::Float32, NoirTypeKind::Bool) => {
                let right_val = self.builder.ins().fcvt_to_sint(types::F32, right_val);
                (left_val, right_val, NoirTypeKind::Float32)
            }
            (NoirTypeKind::Bool, NoirTypeKind::Int32) => (left_val, right_val, NoirTypeKind::Int32),
            (NoirTypeKind::Bool, NoirTypeKind::Float32) => {
                let left_val = self.builder.ins().fcvt_to_sint(types::F32, left_val);
                (left_val, right_val, NoirTypeKind::Float32)
            }
            e => panic!(
                "Error during jit compiling, {:?} and {:?} are not compatible",
                e.0, e.1
            ),
        }
    }

    fn handle_int_comparison(&mut self, left_val: Value, right_val: Value, op: IntCC) -> Value {
        let val = self.builder.ins().icmp(op, left_val, right_val);
        self.builder.ins().uextend(types::I32, val)
    }

    fn handle_float_comparison(&mut self, left_val: Value, right_val: Value, op: FloatCC) -> Value {
        let val = self.builder.ins().fcmp(op, left_val, right_val);
        self.builder.ins().uextend(types::I32, val)
    }
}

#[cfg(test)]
pub mod test {
    use cranelift::{
        codegen::ir::{types, Function, InstBuilder},
        frontend::{FunctionBuilder, FunctionBuilderContext},
    };

    use crate::{
        data_type::{
            noir_type::{NoirType, NoirTypeKind},
            schema::Schema,
        },
        optimization::dsl::{expressions::*, jit::JitCompiler},
    };

    use super::ExprTranslator;

    #[test]
    fn test_ensure_compatible_types() {
        let mut func = Function::new();
        let mut func_builder_ctx = FunctionBuilderContext::new();
        let mut func_builder = FunctionBuilder::new(&mut func, &mut func_builder_ctx);
        let block = func_builder.create_block();
        func_builder.append_block_params_for_function_params(block);
        func_builder.switch_to_block(block);
        let mut translator = ExprTranslator {
            params: vec![NoirTypeKind::Int32, NoirTypeKind::Float32],
            builder: func_builder,
        };

        let dummy_val = translator.builder.ins().iconst(types::I32, 0);

        let (_, _, res) = translator.ensure_compatible_types(
            dummy_val,
            dummy_val,
            NoirTypeKind::Int32,
            NoirTypeKind::Int32,
        );
        assert_eq!(res, NoirTypeKind::Int32);

        let (_, _, res) = translator.ensure_compatible_types(
            dummy_val,
            dummy_val,
            NoirTypeKind::Float32,
            NoirTypeKind::Float32,
        );
        assert_eq!(res, NoirTypeKind::Float32);

        let (_, _, res) = translator.ensure_compatible_types(
            dummy_val,
            dummy_val,
            NoirTypeKind::Bool,
            NoirTypeKind::Bool,
        );
        assert_eq!(res, NoirTypeKind::Bool);

        let (_, _, res) = translator.ensure_compatible_types(
            dummy_val,
            dummy_val,
            NoirTypeKind::Int32,
            NoirTypeKind::Float32,
        );
        assert_eq!(res, NoirTypeKind::Float32);

        let (_, _, res) = translator.ensure_compatible_types(
            dummy_val,
            dummy_val,
            NoirTypeKind::Int32,
            NoirTypeKind::Bool,
        );
        assert_eq!(res, NoirTypeKind::Int32);

        let (_, _, res) = translator.ensure_compatible_types(
            dummy_val,
            dummy_val,
            NoirTypeKind::Float32,
            NoirTypeKind::Int32,
        );
        assert_eq!(res, NoirTypeKind::Float32);

        let (_, _, res) = translator.ensure_compatible_types(
            dummy_val,
            dummy_val,
            NoirTypeKind::Float32,
            NoirTypeKind::Bool,
        );
        assert_eq!(res, NoirTypeKind::Float32);

        let (_, _, res) = translator.ensure_compatible_types(
            dummy_val,
            dummy_val,
            NoirTypeKind::Bool,
            NoirTypeKind::Int32,
        );
        assert_eq!(res, NoirTypeKind::Int32);

        let (_, _, res) = translator.ensure_compatible_types(
            dummy_val,
            dummy_val,
            NoirTypeKind::Bool,
            NoirTypeKind::Float32,
        );
        assert_eq!(res, NoirTypeKind::Float32);
    }

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
