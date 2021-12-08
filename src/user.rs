use serde::{Deserialize, Serialize};
use std::hash::{Hasher, Hash};

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
