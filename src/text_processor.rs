use std::fs;
use futures::executor::{ThreadPool, block_on};
use std::thread::spawn;
use std::collections::{HashSet, HashMap};
use futures::task::SpawnExt;
use crate::message::{Messages, User};
use std::iter::FromIterator;
use futures::FutureExt;
use regex::Regex;
use std::str::FromStr;
use std::cmp::max;

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

pub fn walk_messages(dir: &str) {
    let threads = max(1, num_cpus::get());
    let dir = fs::read_dir(dir).expect("Cannot read directory");

    let thread_pool = ThreadPool::new().expect("Cannot create thread pool");

    let mut jobs = vec![];

    for x in dir {
        let x = x.expect("Cannot read directory entry");
        let path = x.path();
        let job = async move {
            let str = fs::read_to_string(&path).expect("Cannot read file");
            let messages: Messages = serde_json::from_str(&str).expect(&format!("Cannot parse file {:?}", path));
            messages.messages.iter().map(|x| x.sender.to_string()).collect::<Vec<String>>()
        };
        jobs.push(thread_pool.spawn_with_handle(job).expect("Cannot spawn"));
    }

    println!("Running {} jobs on {} threads", jobs.len(), threads);

    let users: HashMap<String, User> = block_on(async move {
        let user_pattern = Regex::new(r"(?P<name>.+)((\((?P<qq>\d{6,})\))|(<(?P<email>.+@.+\..+)>))").unwrap();
        let set: HashSet<String> = HashSet::from_iter(futures::future::join_all(jobs).await.into_iter().flat_map(|x| x));
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
    });

    fs::write(format!("users.json"), serde_json::to_string(&users).expect("Cannot serialize")).expect("Cannot write");

    println!("Parsed and wrote {} users to file", users.len());
}