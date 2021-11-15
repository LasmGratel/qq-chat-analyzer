use std::fs;
use futures::executor::{ThreadPool, block_on};
use std::thread::spawn;
use std::collections::{HashSet, HashMap};
use futures::task::SpawnExt;
use crate::message::{Messages, User};
use std::iter::FromIterator;
use futures::{FutureExt, StreamExt};
use regex::Regex;
use std::str::FromStr;
use std::cmp::max;
use rusqlite::Connection;

pub fn parse_user(sender: String, regex: &Regex) -> Option<User> {
    return if regex.is_match(&sender) {
        let caps = regex.captures(&sender)?;
        let nick = caps.name("name")?.as_str().to_string();
        let qq = caps.name("qq").map(|x| u32::from_str(x.as_str()).expect("Cannot parse"));
        let email = caps.name("email").map(|x| x.as_str());
        Some(User {
            id: email.map(|x| x.to_string()).or(qq.map(|x| x.to_string())).unwrap_or_default(),
            nick: vec![nick],
            email: email.unwrap_or_default().to_string(),
            qq: qq.unwrap_or_default()
        })
    } else {
        None
    }
}

pub fn get_senders() -> Option<HashSet<String>> {
    let conn = Connection::open("msg.db").expect("Cannot open msg.db");
    let mut statement = conn.prepare("SELECT sender FROM messages").unwrap();
    let mut set = HashSet::new();
    let mut rows = statement.query(rusqlite::params![]).unwrap();
    while let Some(row) = rows.next().unwrap() {
        set.insert(row.get::<usize, String>(0).unwrap() as String);
    }
    Some(set)
}

pub fn walk_messages(dir: &str) {

    let users: HashMap<String, User> = {
        let user_pattern = Regex::new(r"(?P<name>.+)((\((?P<qq>\d{6,})\))|(<(?P<email>.+@.+\..+)>))").unwrap();
        let set: HashSet<String> = get_senders().unwrap();
        let mut map: HashMap<String, User> = HashMap::new();
        set.into_iter()
            .map(|x| parse_user(x, &user_pattern))
            .filter(|x| x.is_some()).map(|x| x.unwrap())
            .for_each(|x| if map.contains_key(&x.id) {
                    map.get_mut(&x.id).unwrap().nick.push(x.nick[0].to_string());
                } else {
                    map.insert(x.id.clone(), x);
                }
            );
        map
    };

    fs::write(format!("users.json"), serde_json::to_string_pretty(&users).expect("Cannot serialize")).expect("Cannot write");

    println!("Parsed and wrote {} users to file", users.len());
}