use crate::data_type::noir_data::NoirData;
use crate::data_type::noir_type::NoirType;
use core::panic;
use serde::{Deserialize, Serialize};
use std::{
    fmt::Display,
    ops::{Index, IndexMut},
};

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct StreamItem {
    values: Vec<NoirType>,
    values_from: usize,
}

impl StreamItem {
    pub fn new(values: Vec<NoirType>) -> Self {
        StreamItem {
            values,
            values_from: 0
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

    pub(crate) fn get_key(&self) -> Option<&[NoirType]> {
        if self.values_from == 0 {
            None
        } else {
            Some(&self.values[0..self.values_from])
        }
    }

    pub(crate) fn get_value(&self) -> &[NoirType] {
        if self.values_from == 0 {
            &self.values
        } else {
            &self.values[self.values_from..self.values.len()]
        }
    }

    pub fn len(&self) -> usize {
        if self.values_from == 0 {
            self.values.len() - self.values_from
        } else {
            self.values_from
        }
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
        if index >= self.values_from && index < self.values.len() {
            panic!("Cannot access key")
        }

        if index >= self.values.len() {
            panic!("Index {} out of bounds for {}", index, self)
        }

        &self.values[index]
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
