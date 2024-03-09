use crate::data_type::noir_type::NoirType;
use crate::stream::KeyedItem;
use crate::{data_type::noir_data::NoirData, optimization::dsl::expressions::ExprEvaluable};
use core::panic;
use serde::{Deserialize, Serialize};
use std::ops::{AddAssign, Div, DivAssign};
use std::{
    fmt::Display,
    ops::{Index, IndexMut},
};

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct StreamItem {
    values: Vec<NoirType>,
    values_from: usize,
}

impl KeyedItem for StreamItem {
    type Key = usize;

    type Value = Vec<NoirType>;

    fn key(&self) -> &Self::Key {
        &self.values_from
    }

    fn value(&self) -> &Self::Value {
        &self.values
    }

    fn into_kv(self) -> (Self::Key, Self::Value) {
        (self.values_from, self.values)
    }
}

impl StreamItem {
    pub fn new(values: Vec<NoirType>) -> Self {
        StreamItem {
            values,
            values_from: 0,
        }
    }

    pub fn new_with_key(values: Vec<NoirType>, key_len: usize) -> Self {
        StreamItem {
            values,
            values_from: key_len,
        }
    }

    pub(crate) fn absorb_key(self, mut keys_columns: Vec<NoirType>) -> StreamItem {
        if self.values_from == 0 {
            let len = keys_columns.len();
            keys_columns.extend(self.values);
            StreamItem::new_with_key(keys_columns, len)
        } else {
            panic!("StreamItem already has a key")
        }
    }

    pub(crate) fn drop_key(self) -> StreamItem {
        if self.values_from == 0 {
            self
        } else {
            StreamItem::new(self.values[self.values_from..self.values.len()].to_vec())
        }
    }

    pub fn get_key(&self) -> Option<Vec<NoirType>> {
        if self.values_from == 0 {
            None
        } else {
            Some(self.values[0..self.values_from].to_vec())
        }
    }

    pub fn get_value(&self) -> &[NoirType] {
        &self.values[self.values_from..self.values.len()]
    }

    pub fn len(&self) -> usize {
        self.values.len() - self.values_from
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

impl From<StreamItem> for Vec<NoirType> {
    fn from(data: StreamItem) -> Self {
        if data.values_from == 0 {
            data.values
        } else {
            data.values[data.values_from..data.values.len()].to_vec()
        }
    }
}

impl From<NoirData> for StreamItem {
    fn from(data: NoirData) -> Self {
        match data {
            NoirData::Row(row) => StreamItem::new(row),
            NoirData::NoirType(t) => StreamItem::new(vec![t]),
        }
    }
}

impl From<Vec<NoirType>> for StreamItem {
    fn from(data: Vec<NoirType>) -> Self {
        StreamItem::new(data)
    }
}

impl From<(Vec<NoirType>, NoirData)> for StreamItem {
    fn from(mut data: (Vec<NoirType>, NoirData)) -> Self {
        match data.1 {
            NoirData::Row(row) => {
                let len = data.0.len();
                data.0.extend(row);
                StreamItem::new_with_key(data.0, len)
            }
            NoirData::NoirType(t) => StreamItem::new_with_key([vec![t], data.0].concat(), 1),
        }
    }
}

impl From<(Vec<NoirType>, (StreamItem, StreamItem))> for StreamItem {
    fn from(data: (Vec<NoirType>, (StreamItem, StreamItem))) -> Self {
        let (mut key, (left, right)) = data;
        let mut data: Vec<NoirType> = Vec::with_capacity(left.len() + right.len());
        data.extend(left.get_value());
        data.extend(right.get_value());
        let len = key.len();
        key.extend(data);
        StreamItem::new_with_key(key, len)
    }
}

impl Display for StreamItem {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.values_from == 0 {
            write!(f, "StreamItem({:?})", self.values)
        } else {
            write!(
                f,
                "StreamItem(K: {:?}, V: {:?})",
                self.get_key().unwrap(),
                self.get_value()
            )
        }
    }
}

impl Index<usize> for StreamItem {
    type Output = NoirType;

    fn index(&self, index: usize) -> &Self::Output {
        &self.values[index + self.values_from]
    }
}

impl IndexMut<usize> for StreamItem {
    fn index_mut(&mut self, index: usize) -> &mut Self::Output {
        if index >= self.values_from && index < self.values.len() {
            panic!("Cannot modify key")
        }

        if index >= self.values.len() {
            panic!("Index {} out of bounds for {}", index, self)
        }

        &mut self.values[index]
    }
}

impl ExprEvaluable for StreamItem {
    fn as_ptr(&self) -> *const NoirType {
        unsafe { self.values.as_ptr().add(self.values_from) }
    }
}

impl<T: Into<StreamItem>> AddAssign<T> for StreamItem {
    /// Add pariwise values
    fn add_assign(&mut self, rhs: T) {
        let rhs = rhs.into();
        if self.len() != rhs.len() {
            panic!("Cannot add StreamItems of different lengths")
        }
        for (l, r) in self
            .values
            .iter_mut()
            .skip(self.values_from)
            .zip(rhs.values.iter().skip(rhs.values_from))
        {
            *l += *r;
        }
    }
}

impl DivAssign<f64> for StreamItem {
    fn div_assign(&mut self, rhs: f64) {
        for l in self.values.iter_mut().skip(self.values_from) {
            *l /= rhs;
        }
    }
}

impl Div<f64> for StreamItem {
    type Output = StreamItem;

    fn div(mut self, rhs: f64) -> Self::Output {
        for l in self.values.iter_mut().skip(self.values_from) {
            *l /= rhs;
        }
        self
    }
}
