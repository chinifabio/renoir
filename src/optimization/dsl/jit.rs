use cranelift::prelude::*;
use cranelift_jit::{JITBuilder, JITModule};
use cranelift_module::{DataDescription, Linkage, Module};
use std::collections::HashMap;
use std::slice;

use crate::data_type::noir_type::{NoirType, NoirTypeKind};
use crate::data_type::schema::Schema;

use super::expressions::{BinaryOp, Expr};

pub struct JIT {
    builder_context: FunctionBuilderContext,
    ctx: codegen::Context,
    module: JITModule,
    data_description: DataDescription,
}

impl Default for JIT {
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
            data_description: DataDescription::new(),
            module,
        }
    }
}

impl JIT {
    pub fn compile(&mut self, input_expr: Expr, schema: Schema) -> Result<*const u8, String> {
        let params = schema.columns.clone();
        let the_return = input_expr.evaluate(&schema);
        self.translate(params, the_return.into(), input_expr)?;
        Err("Not implemented".to_string())
    }

    fn translate(
        &mut self,
        params: Vec<NoirTypeKind>,
        the_return: NoirTypeKind,
        expr: Expr,
    ) -> Result<(), String> {
        for p in &params {
            let abi_param = match p {
                NoirTypeKind::Int32 => AbiParam::new(types::I32),
                NoirTypeKind::Float32 => AbiParam::new(types::F32),
                NoirTypeKind::Bool => AbiParam::new(types::I8),
                e => return Err(format!("Error during jit compiling, {} is not supported", e)),
            };
            self.ctx.func.signature.params.push(abi_param);
        }

        let abi_return = match the_return {
            NoirTypeKind::Int32 => types::I32,
            NoirTypeKind::Float32 => types::F32,
            NoirTypeKind::Bool => types::I8,
            e => return Err(format!("Error during jit compiling, {} is not supported", e)),
        };
        self.ctx.func.signature.returns.push(AbiParam::new(abi_return));

        let mut builder = FunctionBuilder::new(&mut self.ctx.func, &mut self.builder_context);
        let entry_block = builder.create_block();
        builder.append_block_params_for_function_params(entry_block);
        builder.switch_to_block(entry_block);
        builder.seal_block(entry_block);

        let variables = declare_variables(&mut builder, &params, the_return, entry_block);

        let mut translator = ExprTranslator {
            params,
            builder,
            variables,
            module: &mut self.module,
        };

        let (return_value, computed_return_type) = translator.translate(expr);
        assert_eq!(computed_return_type, the_return);

        translator.builder.ins().return_(&[return_value]);
        translator.builder.finalize();

        Ok(())
    }
}

struct ExprTranslator<'a> {
    params: Vec<NoirTypeKind>,
    builder: FunctionBuilder<'a>,
    variables: HashMap<String, Variable>,
    module: &'a mut JITModule,
}

impl<'a> ExprTranslator<'a> {
    fn translate(&mut self, expr: Expr) -> (Value, NoirTypeKind) {
        match expr {
            Expr::NthColumn(n) => {
                let var = self.variables[&format!("param{}", n)];
                let val = self.builder.use_var(var);
                (val, self.params[n])
            },
            Expr::Literal(v) => {
                match v {
                    NoirType::Int32(v) => (self.builder.ins().iconst(types::I32, v as i64), NoirTypeKind::Int32),
                    NoirType::Float32(v) => (self.builder.ins().f32const(v), NoirTypeKind::Float32),
                    NoirType::Bool(v) => (self.builder.ins().iconst(types::I8, if v { 1 } else { 0 }), NoirTypeKind::Bool),
                    e => panic!("Error during jit compiling, {} is not supported", e),
                }
            },
            Expr::BinaryExpr { left, op, right } => {
                let (left_val, left_type) = self.translate(*left);
                let (right_val, right_type) = self.translate(*right);
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
                    },
                    e => panic!("Error during jit compiling, {:?} is not supported yet", e),
                }
            },
            Expr::UnaryExpr { op, expr } => todo!(),
            Expr::AggregateExpr { op, expr } => todo!(),
            Expr::Empty => todo!(),
        }
    }
}

fn declare_variables(
    builder: &mut FunctionBuilder,
    params: &[NoirTypeKind],
    the_return: NoirTypeKind,
    entry_block: Block,
) -> HashMap<String, Variable> {
    let mut variables = HashMap::new();
    let mut index = 0;

    for (i, var_type) in params.iter().enumerate() {
        let var_type = match var_type {
            NoirTypeKind::Int32 => types::I32,
            NoirTypeKind::Float32 => types::F32,
            NoirTypeKind::Bool => types::I8,
            e => panic!("Error during jit compiling, {} is not supported", e),
        };
        let val = builder.block_params(entry_block)[i];
        let var = declare_variable(var_type, builder, &mut variables, &mut index, &format!("param{}", i));
        builder.def_var(var, val);
    }

    let return_type = match the_return {
        NoirTypeKind::Int32 => types::I32,
        NoirTypeKind::Float32 => types::F32,
        NoirTypeKind::Bool => types::I8,
        e => panic!("Error during jit compiling, {} is not supported", e),
    };
    let return_variable = declare_variable(return_type, builder, &mut variables, &mut index, "return");

    variables
}

fn declare_variable(
    var_type: types::Type,
    builder: &mut FunctionBuilder,
    variables: &mut HashMap<String, Variable>,
    index: &mut usize,
    name: &str,
) -> Variable {
    let var = Variable::new(*index);
    if !variables.contains_key(name) {
        variables.insert(name.into(), var);
        builder.declare_var(var, var_type);
        *index += 1;
    }
    var
}