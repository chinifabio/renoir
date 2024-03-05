use serde::{Deserialize, Serialize};

use crate::data_type::noir_type::NoirType;

pub trait AggregateState {
    fn accumulate(&mut self, value: NoirType);
    fn include(&mut self, other: Self);
    fn finalize(&self) -> NoirType;
    fn new() -> Self
    where
        Self: Sized;
}
#[derive(Clone, PartialEq, Eq, Debug, Copy, Hash, Serialize, Deserialize)]
pub struct Val {
    value: Option<NoirType>,
}

#[derive(Clone, PartialEq, Eq, Debug, Copy, Hash, Serialize, Deserialize)]
pub struct Sum {
    sum: NoirType,
}

#[derive(Clone, PartialEq, Eq, Debug, Copy, Hash, Serialize, Deserialize)]
pub struct Count {
    count: i32,
}

#[derive(Clone, PartialEq, Eq, Debug, Copy, Hash, Serialize, Deserialize)]
pub struct Max {
    max: Option<NoirType>,
}

#[derive(Clone, PartialEq, Eq, Debug, Copy, Hash, Serialize, Deserialize)]
pub struct Min {
    min: Option<NoirType>,
}

#[derive(Clone, PartialEq, Eq, Debug, Copy, Hash, Serialize, Deserialize)]
pub struct Avg {
    sum: NoirType,
    count: i32,
}

impl AggregateState for Val {
    fn accumulate(&mut self, value: NoirType) {
        match self.value {
            Some(v) => {
                if v != value {
                    panic!("Value mismatch! this is because you are accumulating a non-aggregate column. Please use a different aggregation function.");
                }
            }
            None => self.value = Some(value),
        }
    }

    fn finalize(&self) -> NoirType {
        self.value.unwrap()
    }

    fn new() -> Self {
        Val { value: None }
    }

    fn include(&mut self, other: Self) {
        match (self.value, other.value) {
            (Some(v1), Some(v2)) => {
                if v1 != v2 {
                    panic!("Value mismatch! this is because you are accumulating a non-aggregate column. Please use a different aggregation function.");
                }
            }
            (Some(_), None) => panic!("Value mismatch! this is because you are accumulating a non-aggregate column. Please use a different aggregation function."),
            (None, Some(v)) => self.value = Some(v),
            (None, None) => {}
        }
    }
}

impl AggregateState for Sum {
    fn accumulate(&mut self, value: NoirType) {
        self.sum += value;
    }

    fn finalize(&self) -> NoirType {
        self.sum
    }

    fn new() -> Self {
        Sum {
            sum: NoirType::Int32(0),
        }
    }

    fn include(&mut self, other: Self) {
        self.sum += other.sum;
    }
}

impl AggregateState for Count {
    fn accumulate(&mut self, _value: NoirType) {
        self.count += 1;
    }

    fn finalize(&self) -> NoirType {
        NoirType::Int32(self.count)
    }

    fn new() -> Self {
        Count { count: 0 }
    }

    fn include(&mut self, other: Self) {
        self.count += other.count;
    }
}

impl AggregateState for Max {
    fn accumulate(&mut self, value: NoirType) {
        if let Some(max) = self.max {
            if value > max {
                self.max = Some(value);
            }
        } else {
            self.max = Some(value);
        }
    }

    fn finalize(&self) -> NoirType {
        self.max.unwrap()
    }

    fn new() -> Self {
        Max { max: None }
    }

    fn include(&mut self, other: Self) {
        match (self.max, other.max) {
            (Some(v1), Some(v2)) => {
                if v1 >= v2 {
                    self.max = Some(v1);
                } else {
                    self.max = Some(v2);
                }
            }
            (Some(_), None) => {}
            (None, Some(v)) => self.max = Some(v),
            (None, None) => {}
        }
    }
}

impl AggregateState for Min {
    fn accumulate(&mut self, value: NoirType) {
        if let Some(min) = self.min {
            if value < min {
                self.min = Some(value);
            }
        } else {
            self.min = Some(value);
        }
    }

    fn finalize(&self) -> NoirType {
        self.min.unwrap()
    }

    fn new() -> Self {
        Min { min: None }
    }

    fn include(&mut self, other: Self) {
        match (self.min, other.min) {
            (Some(v1), Some(v2)) => {
                if v1 <= v2 {
                    self.min = Some(v1);
                } else {
                    self.min = Some(v2);
                }
            }
            (Some(_), None) => {}
            (None, Some(v)) => self.min = Some(v),
            (None, None) => {}
        }
    }
}

impl AggregateState for Avg {
    fn accumulate(&mut self, value: NoirType) {
        self.sum += value;
        self.count += 1;
    }

    fn finalize(&self) -> NoirType {
        self.sum / NoirType::Int32(self.count)
    }

    fn new() -> Self {
        Avg {
            sum: NoirType::Int32(0),
            count: 0,
        }
    }

    fn include(&mut self, other: Self) {
        self.sum += other.sum;
        self.count += other.count;
    }
}
