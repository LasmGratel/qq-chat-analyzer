use std::{fs, io};
use std::path::Path;
use std::fs::File;
use std::io::{BufRead, Seek};
use regex::Regex;
use std::sync::{Arc};
use indicatif::{ProgressBar, ProgressIterator, ProgressStyle};
use futures::channel::mpsc;
use anyhow::Error;
use uuid::Uuid;
use crate::message::{Message};
use std::cmp::max;
use std::thread::spawn;
use std::time::Instant;
use std::str::FromStr;
use sqlx::{Connection, AnyPool, Executor, Transaction, Any, AnyConnection};
use futures::AsyncBufReadExt;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::rc::Rc;
use std::cell::{RefCell, RefMut};
use chrono::{DateTime, Local, Utc, TimeZone};

#[derive(Default)]
struct Subject {
    email: String,
    is_group: bool,
    qq: i32,
    grouping: String,
    nick: Vec<String>
}

async fn insert_messages(messages: &Vec<Message>, conn: &mut AnyConnection) -> Result<(), sqlx::Error> {
    for message in messages.into_iter() {
        sqlx::query("INSERT INTO messages (subject, grouping, sender, time, text) VALUES (?, ?, ?, ?, ?)")
            .bind(&message.subject)
            .bind(&message.grouping)
            .bind(&message.sender)
            .bind(&message.time)
            .bind(&message.text)
            .execute(&mut *conn)
            .await?;
    }
    Ok(())
}

async fn walk_lines(path: &Path, conn: &mut AnyConnection, buffer_size: usize) -> Result<(), sqlx::Error> {
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
    let mut reader = io::BufReader::new(file);

    let progress_bar = ProgressBar::new(total_lines as u64);
    progress_bar.set_style(ProgressStyle::default_bar()
        .template("[{eta_precise}] {bar:40.cyan/blue} {pos:>7}/{len:7} {msg}")
        .progress_chars("##-"));

    sqlx::query("DROP TABLE IF EXISTS messages").execute(&mut *conn).await?;

    sqlx::query("CREATE TABLE messages (
              id              INTEGER PRIMARY KEY AUTOINCREMENT ,
              subject INTEGER not null ,
              grouping INTEGER not null ,
              sender TEXT not null ,
              time INTEGER not null ,
              text TEXT NOT NULL
              );

CREATE TABLE subjects (
    id INTEGER PRIMARY KEY AUTOINCREMENT ,
    is_group integer not null ,
    name TEXT NOT NULL
);

CREATE TABLE groupings (
    id INTEGER PRIMARY KEY AUTOINCREMENT ,
    name TEXT NOT NULL
);

              PRAGMA journal_mode = WAL;
              PRAGMA synchronous = NORMAL;")
        .execute(&mut *conn)
        .await?;

    // let placeholders: String = (0..buffer_size).into_iter().map(|_| "(?, ?, ?, ?, ?, ?)").collect::<Vec<_>>().join(", ");

    let mut subjects: Vec<(bool, String)> = vec![];
    let mut groupings: Vec<String> = vec![];
    let mut subject_map: HashMap<String, usize> = HashMap::new();
    let mut grouping_map: HashMap<String, usize> = HashMap::new();

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
        } else {
            if group.is_some() && subject.is_some() {
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

                    let grouping = if grouping_map.contains_key(&group) {
                        *grouping_map.get(&group).unwrap()
                    } else {
                        groupings.push(group.clone());
                        grouping_map.insert(group, groupings.len() - 1);
                        groupings.len() - 1
                    };
                    if !trim_line.is_empty() {
                        let time = Local.datetime_from_str(&time, "%Y-%m-%d %H:%M:%S").unwrap();
                        let message = Message {
                            subject: subject as i64,
                            sender: sender.clone(),
                            grouping: grouping as i64,
                            time: time.timestamp(),
                            text: line.to_string()
                        };
                        buffer.push(message);

                        if buffer.len() == buffer_size {
                            insert_messages(&buffer, conn).await?;
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
    }

    // drop(db_tx);
    progress_bar.finish();
    
    subject_map.clear();
    grouping_map.clear();
    for (is_group, name) in subjects.into_iter() {
        sqlx::query("INSERT INTO subjects (is_group, name) VALUES (?, ?)")
            .bind(is_group)
            .bind(name)
            .execute(&mut *conn)
            .await?;
    }
    for name in groupings.into_iter() {
        sqlx::query("INSERT INTO groupings (name) VALUES (?)")
            .bind(name)
            .execute(&mut *conn)
            .await?;
    }

    println!("Parsing complete");
    // db_thread.join();

    Ok(())
}

const LF: u8 = '\n' as u8;

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

pub async fn analyze_text<P>(path: P, conn: &mut AnyConnection, buffer_size: &str) -> Result<(), sqlx::Error>
    where P: AsRef<Path> {
    walk_lines(path.as_ref(), conn, usize::from_str(buffer_size).unwrap()).await
}

fn read_lines<P>(filename: P) -> io::Result<io::Lines<io::BufReader<File>>>
    where P: AsRef<Path>, {
    let file = File::open(filename)?;
    Ok(io::BufReader::new(file).lines())
}