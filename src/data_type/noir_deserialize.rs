use core::fmt;

use serde::{de::Visitor, Deserialize, Deserializer};

use crate::data_type::NoirType;

use super::NoirData;

impl<'de> Deserialize<'de> for NoirData {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
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
                let single_data: NoirType;

                if let Ok(Some(value)) = seq.next_element::<String>() {
                    if let Ok(int_value) = value.parse::<i32>() {
                        single_data = NoirType::Int32(int_value);
                    } else if let Ok(float_value) = value.parse::<f32>() {
                        single_data = NoirType::Float32(float_value);
                    } else {
                        single_data = NoirType::None();
                    }
                } else {
                    return Ok(NoirData::NoirType(NoirType::None()));
                }

                match seq.next_element::<String>() {
                    Ok(Some(value)) => {
                        let mut value = value;
                        data.push(single_data);
                        loop {
                            if let Ok(int_value) = value.parse::<i32>() {
                                data.push(NoirType::Int32(int_value));
                            } else if let Ok(float_value) = value.parse::<f32>() {
                                data.push(NoirType::Float32(float_value));
                            } else {
                                data.push(NoirType::None());
                            }
                            value = match seq.next_element::<String>() {
                                Ok(Some(value)) => value,
                                _ => return Ok(NoirData::Row(data)),
                            };
                        }
                    }
                    _ => Ok(NoirData::NoirType(single_data)),
                }
            }

            fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
            where
                A: serde::de::MapAccess<'de>,
            {
                let mut data: Vec<NoirType> = Vec::new();
                let single_data: NoirType;

                if let Ok(value) = map.next_value::<String>() {
                    if let Ok(int_value) = value.parse::<i32>() {
                        single_data = NoirType::Int32(int_value);
                    } else if let Ok(float_value) = value.parse::<f32>() {
                        single_data = NoirType::Float32(float_value);
                    } else {
                        single_data = NoirType::None();
                    }
                } else {
                    return Ok(NoirData::NoirType(NoirType::None()));
                }

                match map.next_value::<String>() {
                    Ok(value) => {
                        let mut value = value;
                        data.push(single_data);
                        loop {
                            if let Ok(int_value) = value.parse::<i32>() {
                                data.push(NoirType::Int32(int_value));
                            } else if let Ok(float_value) = value.parse::<f32>() {
                                data.push(NoirType::Float32(float_value));
                            } else {
                                data.push(NoirType::None());
                            }
                            value = match map.next_value::<String>() {
                                Ok(value) => value,
                                _ => return Ok(NoirData::Row(data)),
                            };
                        }
                    }
                    _ => Ok(NoirData::NoirType(single_data)),
                }
            }
        }

        deserializer.deserialize_map(__NoirDataVisitor {})
    }
}
