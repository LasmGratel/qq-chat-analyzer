use std::collections::{HashSet, HashMap};
use std::iter::FromIterator;
use regex::Regex;
use std::str::FromStr;

use sqlx::{AnyPool, Error, Row, AnyConnection, Any};

use crate::user::User;
use itertools::Itertools;

pub fn parse_user(sender: String, regex: &Regex) -> Option<User> {
    return if regex.is_match(&sender) {
        let caps = regex.captures(&sender)?;
        let nick = caps.name("name")?.as_str().to_string();
        let qq = caps.name("qq").map(|x| u32::from_str(x.as_str()).expect("Cannot parse"));
        let email = caps.name("email").map(|x| x.as_str());
        Some(User {
            id: email.map(|x| x.to_string()).or_else(|| qq.map(|x| x.to_string())).unwrap_or_default(),
            nick: vec![nick],
            email: email.unwrap_or_default().to_string(),
            qq: qq.unwrap_or_default()
        })
    } else {
        None
    }
}

pub async fn get_senders(conn: &AnyPool) -> Result<HashSet<String>, Error> {
    sqlx::query("SELECT DISTINCT sender FROM messages")
        .fetch_all(conn)
        .await
        .map(|x| HashSet::from_iter(x.into_iter().map(|y| y.get::<String, usize>(0))))
}

pub async fn get_user_subjects(conn: &mut AnyConnection) -> Result<HashSet<String>, Error> {
    sqlx::query("SELECT name FROM subjects WHERE is_group=1")
        .fetch_all(conn)
        .await
        .map(|x| HashSet::from_iter(x.into_iter().map(|y| y.get::<String, usize>(0))))
}

pub async fn walk_messages(pool: AnyPool) -> Result<(), Error> {
    sqlx::query("
DROP TABLE IF EXISTS users;
DROP TABLE IF EXISTS nicks;
DROP TABLE IF EXISTS subjects;
CREATE TABLE users (
    id INTEGER PRIMARY KEY AUTO_INCREMENT,
    qqid TEXT NOT NULL,
    nick TEXT NOT NULL,
    email TEXT,
    qq INTEGER
);

CREATE TABLE nicks (
    id INTEGER PRIMARY KEY AUTO_INCREMENT,
    value TEXT NOT NULL,
    user_id INTEGER
);

CREATE TABLE subjects (
    id INTEGER PRIMARY KEY AUTO_INCREMENT ,
    is_group integer not null ,
    name TEXT NOT NULL
);

")
        /*
PRAGMA journal_mode = WAL;
PRAGMA synchronous = NORMAL;*/
        .execute(&pool)
        .await?;

    let users: HashMap<String, User> = {
        let user_pattern = Regex::new(r"(?P<name>.+)((\((?P<qq>\d{6,})\))|(<(?P<email>.+@.+\..+)>))").unwrap();
        let set: HashSet<String> = get_senders(&pool).await.unwrap();

        let mut map: HashMap<String, User> = HashMap::new();
        set.into_iter()
            .map(|x| parse_user(x, &user_pattern))
            .flatten()
            .for_each(|x| if map.contains_key(&x.id) {
                    map.get_mut(&x.id).unwrap().nick.push(x.nick[0].to_string());
                } else {
                    map.insert(x.id.clone(), x);
                }
            );
        map
    };

    insert_users(users.into_values().collect(), pool).await?;

    // fs::write(format!("users.json"), serde_json::to_string_pretty(&users).expect("Cannot serialize")).expect("Cannot write");

    Ok(())
}

async fn insert_users(users: Vec<User>, conn: AnyPool) -> Result<(), Error> {
    for (i, user) in users.into_iter().enumerate() {
        sqlx::query::<Any>("INSERT INTO users (qqid, nick, email, qq) VALUES (?, ?, ?, ?)")
            .bind(&user.id)
            .bind(user.nick.iter().join("\n"))
            .bind(&user.email)
            .bind(user.qq as i64)
            .execute(&conn)
            .await?;
        for nick in user.nick.iter() {
            sqlx::query("INSERT INTO nicks (value, user_id) VALUES (?, ?)")
                .bind(nick)
                .bind(i as i64)
                .execute(&conn)
                .await?;
        }
    }
    Ok(())

}