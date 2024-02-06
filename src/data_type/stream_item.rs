use crate::data_type::noir_type::NoirType;
use crate::data_type::noir_data::NoirData;
use core::panic;
use serde::{Deserialize, Serialize};
use std::{fmt::Display, ops::{Index, IndexMut}};

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct StreamItem {
    values: Vec<NoirType>,
    keys_from: i32,
}

impl StreamItem {
    pub fn new(values: Vec<NoirType>) -> Self {
        StreamItem {
            values,
            keys_from: -1,
        }
    }

    pub fn new_with_key(values: Vec<NoirType>, i: i32) -> Self {
        StreamItem {
            values,
            keys_from: i,
        }
    }

    pub(crate) fn absorb_key(self, keys_columns: Vec<NoirType>) -> StreamItem {
        if self.keys_from < 0 {
            let len = self.values.len() as i32;
            StreamItem::new_with_key([self.values, keys_columns].concat(), len)
        } else {
            // StreamItem::new_with_key([self.values, k].concat(), self.keys_from)
            panic!("StreamItem already has a key")
        }
    }

    pub(crate) fn drop_key(self) -> StreamItem {
        if self.keys_from < 0 {
            self
        } else {
            StreamItem::new(self.values[0..self.keys_from as usize].to_vec())
        }
    }

    pub(crate) fn get_key(&self) -> Option<&[NoirType]> {
        if self.keys_from < 0 {
            None
        } else {
            Some(&self.values[self.keys_from as usize..self.values.len()])
        }
    }

    pub(crate) fn get_value(&self) -> &[NoirType] {
        if self.keys_from < 0 {
            &self.values
        } else {
            &self.values[0..self.keys_from as usize]
        }
    }

    pub(crate) fn len(&self) -> usize {
        if self.keys_from < 0 {
            self.values.len()
        } else {
            self.keys_from as usize
        }
    }
}

impl From<StreamItem> for Vec<NoirType> {
    fn from(data: StreamItem) -> Self {
        if data.keys_from < 0 {
            data.values
        } else {
            data.values[0..data.keys_from as usize].to_vec()
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
    fn from(data: (Vec<NoirType>, NoirData)) -> Self {
        match data.1 {
            NoirData::Row(row) => {
                let len = row.len() as i32;
                StreamItem::new_with_key([row, data.0].concat(), len)
            }
            NoirData::NoirType(t) => StreamItem::new_with_key([vec![t], data.0].concat(), 1),
        }
    }
}

impl From<(Vec<NoirType>, (StreamItem, StreamItem))> for StreamItem {
    fn from (data: (Vec<NoirType>, (StreamItem, StreamItem))) -> Self {
        let (key, (left, right)) = data;
        let mut data = Vec::with_capacity(left.len() + right.len());
        data.extend(left.get_value());
        data.extend(right.get_value());
        let len =data.len() as i32;
        StreamItem::new_with_key([data, key].concat(), len)
    }
}

impl Display for StreamItem {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.keys_from < 0 {
            write!(f, "StreamItem({:?})", self.values)
        } else {
            write!(f, "StreamItem(K: {:?}, V: {:?})", self.get_key().unwrap(), self.get_value())
        }
    }
}

impl Index<usize> for StreamItem {
    type Output = NoirType;

    fn index(&self, index: usize) -> &Self::Output {
        if index >= self.keys_from as usize && index < self.values.len() {
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
        if index >= self.keys_from as usize && index < self.values.len() {
            panic!("Cannot modify key")
        }

        if index >= self.values.len() {
            panic!("Index {} out of bounds for {}", index, self)
        }

        &mut self.values[index]
    }
}
