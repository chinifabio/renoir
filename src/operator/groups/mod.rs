use std::{collections::HashMap, sync::Arc};

use parking_lot::Mutex;
use serde::{Deserialize, Serialize};

use crate::CoordUInt;

use super::StreamElement;

pub(crate) mod heartbeat;
pub(crate) mod sink;
pub(crate) mod source;

pub(crate) type GroupName = String;
pub(crate) type GroupMap = Arc<Mutex<HashMap<GroupName, CoordUInt>>>;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct GroupStreamElement<T> {
    element: StreamElement<T>,
    group: Option<GroupName>, // todo forse non serve group name
    id: CoordUInt,
}

impl<T> GroupStreamElement<T> {
    pub fn wrap(element: StreamElement<T>, group: Option<GroupName>, id: CoordUInt) -> Self {
        Self { element, group, id }
    }
}
