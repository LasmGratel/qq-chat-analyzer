use serde::{Deserialize, Serialize};
use std::hash::{Hash, Hasher};

#[derive(Debug, Serialize, Deserialize, Default, Hash, Clone)]
pub struct Message {
    pub subject: i64,
    pub sender: String,
    pub group_id: i64,
    pub time: i64,
    pub text: String
}