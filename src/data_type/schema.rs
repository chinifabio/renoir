use crate::{data_type::noir_type::NoirTypeKind, optimization::dsl::expressions::Expr};
use csv::ReaderBuilder;
use itertools::Itertools;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;

use super::noir_type::NoirType;

#[derive(Clone, Deserialize, Serialize, Debug, PartialEq, Eq, Hash)]
pub struct Schema {
    pub(crate) columns: Vec<NoirTypeKind>,
}

impl Schema {
    pub fn new(columns: Vec<NoirTypeKind>) -> Self {
        Self { columns }
    }

    pub fn same_type(n_columns: usize, t: NoirTypeKind) -> Self {
        Self {
            columns: (0..n_columns).map(|_| t).collect(),
        }
    }

    pub fn infer_from_file(path: PathBuf) -> Self {
        info!("Infering schema from file: {:?}", path);
        let mut csv_reader = ReaderBuilder::new().from_path(path).unwrap();
        let mut record = csv::StringRecord::new();
        let _ = csv_reader.read_record(&mut record);
        let columns = record
            .iter()
            .map(|item| {
                if item.parse::<i32>().is_ok() {
                    NoirTypeKind::Int32
                } else if item.parse::<f32>().is_ok() {
                    NoirTypeKind::Float32
                } else if item.parse::<bool>().is_ok() {
                    NoirTypeKind::Bool
                } else {
                    NoirTypeKind::None
                }
            })
            .collect();
        Self { columns }
    }

    pub(crate) fn merge(self, other: Schema) -> Schema {
        Schema {
            columns: [self.columns, other.columns].concat(),
        }
    }

    pub(crate) fn compute_result_type(&self, expr: &Expr) -> NoirTypeKind {
        expr.evaluate(
            self.columns
                .iter()
                .map(|c| match c {
                    NoirTypeKind::Int32 => NoirType::Int32(0),
                    NoirTypeKind::Float32 => NoirType::Float32(0.0),
                    NoirTypeKind::Bool => NoirType::Bool(false),
                    NoirTypeKind::None => NoirType::None(),
                    NoirTypeKind::NaN => NoirType::NaN(),
                })
                .collect_vec()
                .as_slice(),
        )
        .kind()
    }

    pub(crate) fn with_projections(&self, projections: &Option<Vec<usize>>) -> Schema {
        match projections {
            Some(projections) => Schema {
                columns: projections.iter().map(|i| self.columns[*i]).collect(),
            },
            None => self.clone(),
        }
    }

    pub(crate) fn update(&self, columns: &[Expr]) -> Schema {
        Schema {
            columns: columns
                .iter()
                .map(|c| self.compute_result_type(c))
                .collect(),
        }
    }
}
