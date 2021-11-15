use serde::{Deserialize, Serialize};
use std::hash::{Hash, Hasher};

#[derive(Default, Serialize, Deserialize, Debug)]
pub struct User {
    pub id: String,
    pub nick: Vec<String>,
    pub email: String,
    pub qq: u32
}

impl Hash for User {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.id.hash(state);
    }
}

impl PartialEq for User {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
    }
}

impl Eq for User {}

#[derive(Debug, Serialize, Deserialize)]
pub struct Messages {
    /// 消息分组
    pub group: String,
    pub is_group: bool,
    pub subject: String,

    pub messages: Vec<Message>
}

#[derive(Debug, Serialize, Deserialize, Default, Hash, Clone)]
pub struct Message {
    pub message_group: String,
    pub subject: String,
    pub is_group: bool,
    pub sender: String,
    pub time: String,
    pub text: String
}