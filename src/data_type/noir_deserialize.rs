use core::fmt;

use serde::{de::Visitor, Deserialize, Deserializer};

use crate::data_type::NoirType;

use super::NoirData;

impl<'de> Deserialize<'de> for NoirData {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        enum __Field {
            Row,
            NoirType,
        }

        struct __NoirDataVisitor;

        impl<'de> Visitor<'de> for __NoirDataVisitor {
            type Value = NoirData;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("a struct NoirData")
            }

            fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
            where
                A: serde::de::SeqAccess<'de>,
            {
                let mut data: Vec<NoirType> = Vec::new();

                while let Ok(Some(value)) = seq.next_element::<String>() {
                    if let Ok(int_value) = value.parse::<i32>() {
                        data.push(NoirType::Int32(int_value));
                    } else if let Ok(float_value) = value.parse::<f32>() {
                        data.push(NoirType::Float32(float_value));
                    } else {
                        data.push(NoirType::None());
                    }
                }

                if data.len() == 1 {
                    Ok(NoirData::NoirType(data.remove(0)))
                } else {
                    Ok(NoirData::Row(data))
                }
            }

            fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
            where
                A: serde::de::MapAccess<'de>,
            {
                let mut data: Vec<NoirType> = Vec::new();

                while let Ok(value) = map.next_value::<String>() {
                    if let Ok(int_value) = value.parse::<i32>() {
                        data.push(NoirType::Int32(int_value));
                    } else if let Ok(float_value) = value.parse::<f32>() {
                        data.push(NoirType::Float32(float_value));
                    } else {
                        data.push(NoirType::None());
                    }
                }

                if data.len() == 1 {
                    Ok(NoirData::NoirType(data.remove(0)))
                } else {
                    Ok(NoirData::Row(data))
                }
            }
        }

        deserializer.deserialize_map(__NoirDataVisitor {})
    }
}
