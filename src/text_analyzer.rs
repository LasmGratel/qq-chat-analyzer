use std::{io};
use std::path::Path;
use std::fs::File;
use std::io::{BufRead};
use regex::Regex;
use indicatif::{ProgressBar, ProgressStyle};
use crate::message::{Message};
use std::str::FromStr;
use sqlx::{Executor, Transaction, Any, AnyConnection, AnyPool};
use futures::AsyncBufReadExt;
use std::collections::{HashMap, HashSet};
use chrono::{Local, Utc, TimeZone};
use std::collections::hash_map::Entry;
use futures::executor::{block_on, ThreadPool};
use futures::future::join_all;
use futures::task::SpawnExt;
use itertools::Itertools;

#[derive(Default)]
struct Subject {
    email: String,
    is_group: bool,
    qq: i32,
    group_id: String,
    nick: Vec<String>
}

async fn insert_messages(messages: Vec<Message>, conn: AnyPool) -> Result<(), sqlx::Error> {
    for message in messages.into_iter() {
        sqlx::query("INSERT INTO messages (subject, group_id, sender, time, text) VALUES (?, ?, ?, ?, ?)")
            .bind(&message.subject)
            .bind(&message.group_id)
            .bind(&message.sender)
            .bind(&message.time)
            .bind(&message.text)
            .execute(&conn)
            .await?;
    }
    Ok(())
}

async fn walk_lines(path: &Path, conn: AnyPool, buffer_size: usize) -> Result<(), sqlx::Error> {
    let time_pattern = Regex::new(r"(?P<time>\d\d\d\d-\d\d-\d\d \d\d:\d\d:\d\d) (?P<sender>.+)").unwrap();

    let mut group: Option<String> = None;
    let mut subject: Option<String> = None;
    let mut flag = false;
    let mut sender = String::default();
    let mut time = String::default();

    let mut buffer = Vec::with_capacity(buffer_size);

    let file = File::open(path).expect("Cannot open");
    let total_lines = count_lines(file).expect("Cannot get file lines");
    println!("File has {} lines", total_lines);
    let mut lines_processed = 0usize;

    let file = File::open(path).expect("Cannot open");
    let reader = io::BufReader::new(file);

    let progress_bar = ProgressBar::new(total_lines as u64);
    progress_bar.set_style(ProgressStyle::default_bar()
        .template("[{eta_precise}] {bar:40.cyan/blue} {pos:>7}/{len:7} {msg}")
        .progress_chars("##-"));

    sqlx::query("DROP TABLE IF EXISTS messages;").execute(&conn).await?;
    sqlx::query("DROP TABLE IF EXISTS subjects;").execute(&conn).await?;
    sqlx::query("DROP TABLE IF EXISTS groupings;").execute(&conn).await?;

    sqlx::query("
CREATE TABLE IF NOT EXISTS messages (
    id INTEGER PRIMARY KEY AUTO_INCREMENT,
    subject INTEGER not null,
    group_id INTEGER not null,
    sender TEXT not null,
    time INTEGER not null,
    text TEXT NOT NULL
);")
        .execute(&conn)
        .await?;

    sqlx::query("
CREATE TABLE IF NOT EXISTS subjects (
    id INTEGER PRIMARY KEY AUTO_INCREMENT,
    is_group integer not null,
    name TEXT NOT NULL
);")
        .execute(&conn)
        .await?;

    sqlx::query("
CREATE TABLE IF NOT EXISTS groupings (
    id INTEGER PRIMARY KEY AUTO_INCREMENT,
    name TEXT NOT NULL
);")
        .execute(&conn)
        .await?;

    // let placeholders: String = (0..buffer_size).into_iter().map(|_| "(?, ?, ?, ?, ?, ?)").collect::<Vec<_>>().join(", ");

    let mut subjects: Vec<(bool, String)> = vec![];
    let mut groupings: Vec<String> = vec![];
    let mut subject_map: HashMap<String, usize> = HashMap::new();
    let mut grouping_map: HashMap<String, usize> = HashMap::new();

    let mut jobs = vec![];
    let thread_pool = ThreadPool::new().unwrap();

    for line in reader.lines() {
        lines_processed += 1;
        if lines_processed % buffer_size == 0 {
            progress_bar.inc(buffer_size as u64);
        }

        let line = line.unwrap();
        let trim_line = line.trim();
        if lines_processed == 0 || lines_processed == 1 {
            continue; // Skip
        }
        if trim_line.starts_with("消息分组:") {
            group = Some(trim_line.to_string());
            subject = None;
        } else if trim_line.starts_with("消息对象:") {
            subject = Some(trim_line.to_string());
        } else if group.is_some() && subject.is_some() {
            if flag {
                let group = group.clone().unwrap().strip_prefix("消息分组:").unwrap().to_string();
                let subject = subject.clone().unwrap().strip_prefix("消息对象:").unwrap().to_string();
                let is_group = group == "我的群聊" || group == "已退出的群" || group == "已退出的多人聊天";
                let subject = if subject_map.contains_key(&subject) {
                    *subject_map.get(&subject).unwrap()
                } else {
                    subjects.push((is_group, subject.clone()));
                    subject_map.insert(subject, subjects.len() - 1);
                    subjects.len() - 1
                };

                let group_id = if let Entry::Vacant(e) = grouping_map.entry(group.clone()) {
                    groupings.push(group.clone());
                    e.insert(groupings.len() - 1);
                    groupings.len() - 1
                } else {
                    *grouping_map.get(&group).unwrap()
                };
                if !trim_line.is_empty() {
                    let time = Local.datetime_from_str(&time, "%Y-%m-%d %H:%M:%S").unwrap();
                    let message = Message {
                        subject: subject as i64,
                        sender: sender.clone(),
                        group_id: group_id as i64,
                        time: time.timestamp(),
                        text: line.to_string()
                    };
                    buffer.push(message);

                    if buffer.len() == buffer_size {
                        let to_insert = buffer.clone();
                        let conn = conn.clone();
                        jobs.push(thread_pool.spawn_with_handle(async move {
                            insert_messages(to_insert, conn).await.unwrap();
                            Ok(())
                        }).unwrap());
                        if jobs.len() == 8 {
                            block_on(join_all(jobs));
                        }
                        jobs = vec![];
                        buffer.clear();
                    }
                }

                flag = false;
                continue;
            }

            if time_pattern.is_match(&line) {
                let caps = time_pattern.captures(&line).unwrap();
                let captured_time = caps.name("time").unwrap().as_str();
                let captured_sender = caps.name("sender").unwrap().as_str();
                time = captured_time.to_string();
                sender = captured_sender.to_string();
                flag = true;
                continue;
            }
        }
    }

    // drop(db_tx);
    progress_bar.finish();
    
    subject_map.clear();
    grouping_map.clear();
    for (is_group, name) in subjects.into_iter() {
        let conn = conn.clone();
        jobs.push(thread_pool.spawn_with_handle(async move {
            sqlx::query("INSERT INTO subjects (is_group, name) VALUES (?, ?)")
                .bind(is_group)
                .bind(name)
                .execute(&conn)
                .await.map(|_| ())
        }).unwrap());
    }
    for name in groupings.into_iter() {
        let conn = conn.clone();
        jobs.push(thread_pool.spawn_with_handle(async move {
            sqlx::query("INSERT INTO groupings (name) VALUES (?)")
                .bind(name)
                .execute(&conn)
                .await.map(|_| ())
        }).unwrap());
    }

    println!("Waiting database");
    join_all(jobs.into_iter()).await;

    println!("Parsing complete");
    // db_thread.join();

    Ok(())
}

const LF: u8 = b'\n';

pub fn count_lines<R: io::Read>(handle: R) -> Result<usize, io::Error> {
    let mut reader = io::BufReader::new(handle);
    let mut count = 0;
    let mut line: Vec<u8> = Vec::new();
    while match reader.read_until(LF, &mut line) {
        Ok(n) if n > 0 => true,
        Err(e) => return Err(e),
        _ => false,
    } {
        if *line.last().unwrap() == LF {
            count += 1;
        };
    }
    Ok(count)
}

pub async fn analyze_text<P>(path: P, conn: AnyPool, buffer_size: &str) -> Result<(), sqlx::Error>
    where P: AsRef<Path> {
    walk_lines(path.as_ref(), conn, usize::from_str(buffer_size).unwrap()).await
}

fn read_lines<P>(filename: P) -> io::Result<io::Lines<io::BufReader<File>>>
    where P: AsRef<Path>, {
    let file = File::open(filename)?;
    Ok(io::BufReader::new(file).lines())
}