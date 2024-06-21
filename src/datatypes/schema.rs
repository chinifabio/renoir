use csv::ReaderBuilder;
use serde::{Deserialize, Serialize};
use std::{ops::Index, path::PathBuf};

use crate::dsl::expressions::Expr;

use super::{NoirType, NoirTypeKind};

#[derive(Clone, Deserialize, Serialize, Debug, PartialEq, Eq, Hash)]
pub struct Schema {
    names: Vec<String>,
    columns: Vec<NoirTypeKind>,
}

impl Schema {
    pub fn from_mapping(mapping: Vec<(impl Into<String>, NoirTypeKind)>) -> Self {
        let (names, columns) = mapping.into_iter().map(|(n, c)| (n.into(), c)).unzip();
        Self { names, columns }
    }

    pub fn len(&self) -> usize {
        self.columns.len()
    }

    pub(crate) fn types(&self) -> impl Iterator<Item = &NoirTypeKind> {
        self.columns.iter()
    }

    pub(crate) fn names(&self) -> impl Iterator<Item = &String> {
        self.names.iter()
    }

    pub fn col(&self, name: &str) -> Expr {
        Expr::NthColumn(
            self.names
                .iter()
                .position(|x| x == name)
                .expect(format!("Column '{}' not found", name).as_str()),
        )
    }
}

impl Index<usize> for Schema {
    type Output = NoirTypeKind;

    fn index(&self, index: usize) -> &Self::Output {
        &self.columns[index]
    }
}

impl Index<&str> for Schema {
    type Output = NoirTypeKind;

    fn index(&self, index: &str) -> &Self::Output {
        let idx = self
            .names
            .iter()
            .position(|x| x == index)
            .expect("Column not found");
        &self.columns[idx]
    }
}

impl Schema {
    pub fn new(columns_name: (Vec<NoirTypeKind>, Vec<String>)) -> Self {
        let (columns, names) = columns_name;
        assert_eq!(
            columns.len(),
            names.len(),
            "Columns and names must have the same length"
        );
        Self { names, columns }
    }

    pub fn same_type(n_columns: usize, t: NoirTypeKind) -> Self {
        Self {
            names: (0..n_columns).map(|i| format!("col{}", i)).collect(),
            columns: (0..n_columns).map(|_| t).collect(),
        }
    }

    pub fn infer_from_file(path: PathBuf, has_header: bool) -> Self {
        info!("Infering schema from file: {:?}", path);
        let mut csv_reader = ReaderBuilder::new().from_path(path).unwrap();
        let mut record = csv::StringRecord::new();
        let _ = csv_reader.read_record(&mut record);
        let columns = record
            .iter()
            .map(|item| {
                if item.parse::<i32>().is_ok() {
                    NoirTypeKind::Int
                } else if item.parse::<f32>().is_ok() {
                    NoirTypeKind::Float
                } else if item.parse::<bool>().is_ok() {
                    NoirTypeKind::Bool
                } else {
                    NoirTypeKind::None
                }
            })
            .collect();
        if has_header {
            let names = record.iter().map(|s| s.to_string()).collect();
            Self { names, columns }
        } else {
            let names = (0..columns.len()).map(|i| format!("col{}", i)).collect();
            Self { names, columns }
        }
    }

    pub(crate) fn merge(self, other: Schema) -> Schema {
        Schema {
            names: [
                self.names,
                other
                    .names
                    .into_iter()
                    .map(|n| format!("{}_right", n))
                    .collect(),
            ]
            .concat(),
            columns: [self.columns, other.columns].concat(),
        }
    }

    pub(crate) fn compute_result_type(&self, expr: &Expr) -> NoirTypeKind {
        let input: Vec<NoirType> = self
            .columns
            .iter()
            .map(|c| match c {
                NoirTypeKind::Int => NoirType::Int(0),
                NoirTypeKind::Float => NoirType::Float(0.0),
                NoirTypeKind::Bool => NoirType::Bool(false),
                NoirTypeKind::String => NoirType::String("".to_string()),
                NoirTypeKind::None => NoirType::None(),
            })
            .collect();
        expr.evaluate(&input).kind()
    }

    pub(crate) fn with_projections(&self, projections: &Option<Vec<usize>>) -> Schema {
        match projections {
            Some(projections) => Schema {
                names: projections.iter().map(|i| self.names[*i].clone()).collect(),
                columns: projections.iter().map(|i| self.columns[*i]).collect(),
            },
            None => self.clone(),
        }
    }

    pub(crate) fn update(&self, columns: &[Expr]) -> Schema {
        Schema {
            names: self.names.clone(),
            columns: columns
                .iter()
                .map(|c| self.compute_result_type(c))
                .collect(),
        }
    }
}
