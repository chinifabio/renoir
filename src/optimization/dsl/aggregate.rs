use serde::{Deserialize, Serialize};

use crate::data_type::noir_type::NoirType;

pub trait AggregateState {
    fn aggregate(&mut self, value: NoirType);
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
    fn aggregate(&mut self, value: NoirType) {
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
}

impl AggregateState for Sum {
    fn aggregate(&mut self, value: NoirType) {
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
}

impl AggregateState for Count {
    fn aggregate(&mut self, _value: NoirType) {
        self.count += 1;
    }

    fn finalize(&self) -> NoirType {
        NoirType::Int32(self.count)
    }

    fn new() -> Self {
        Count { count: 0 }
    }
}

impl AggregateState for Max {
    fn aggregate(&mut self, value: NoirType) {
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
}

impl AggregateState for Min {
    fn aggregate(&mut self, value: NoirType) {
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
}

impl AggregateState for Avg {
    fn aggregate(&mut self, value: NoirType) {
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
}
