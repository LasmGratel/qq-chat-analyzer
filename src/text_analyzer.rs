use std::{fs, io};
use std::path::Path;
use std::fs::File;
use std::io::{BufRead, Seek, BufReader};
use regex::Regex;
use std::sync::{Arc};
use indicatif::{ProgressBar, ProgressIterator, ProgressStyle};
use futures::channel::mpsc;
use anyhow::Error;
use uuid::Uuid;
use crate::message::{Messages, Message};
use std::cmp::max;
use std::thread::spawn;
use std::time::Instant;
use std::str::FromStr;
use sqlx::{Connection, AnyPool, Executor, Transaction, Any};

fn read_messages(group: String, subject: String, lines: Vec<&String>, pool: AnyPool) -> Messages {
    let time_pattern = Regex::new(r"(?P<time>\d\d\d\d-\d\d-\d\d \d\d:\d\d:\d\d) (?P<sender>.+)").unwrap();
    let group = group.strip_prefix("消息分组:").unwrap().to_string();
    let subject = subject.strip_prefix("消息对象:").unwrap().to_string();
    let mut messages = Messages {
        is_group: group == "我的群聊" || group == "已退出的群" || group == "已退出的多人聊天",
        group,
        subject,
        messages: vec![]
    };
    let mut flag = false;

    let mut sender = "";
    let mut time = "";

    for line in lines.iter().skip_while(|x| !x.is_empty()) {
        let line = line.trim();
        if time_pattern.is_match(line) {
            let caps = time_pattern.captures(line).unwrap();
            let captured_time = caps.name("time").unwrap().as_str();
            let captured_sender = caps.name("sender").unwrap().as_str();
            time = captured_time;
            sender = captured_sender;
            flag = true;
            continue;
        }
        if flag {
            if !line.is_empty() {
                messages.messages.push(Message {
                    message_group: String::default(),
                    subject: String::default(),
                    is_group: false,
                    sender: sender.to_string(),
                    time: time.to_string(),
                    text: line.to_string()
                });
            }

            flag = false;
            continue;
        }
    }
    messages
}

async fn insert_messages(messages: &Vec<Message>, pool: &AnyPool) -> Result<(), sqlx::Error> {
    let mut transaction: Transaction<Any> = pool.begin().await?;
    for message in messages.into_iter() {
        sqlx::query!("INSERT INTO messages (message_group, subject, is_group, sender, time, text) VALUES (?, ?, ?, ?, ?, ?)",
            &message.message_group, &message.subject, &message.is_group, &message.sender, &message.time, &message.text)
            .execute(&mut transaction)
            .await?;
    }
    Ok(())
}

async fn walk_lines(path: &Path, pool: &AnyPool, buffer_size: usize) {
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

    sqlx::query("DROP TABLE IF EXISTS messages").execute(pool).await.expect("Cannot drop table");

    sqlx::query("CREATE TABLE messages (
                  id              INTEGER PRIMARY KEY,
                  message_group TEXT not null ,
                  subject TEXT not null ,
                  is_group integer not null ,
                  sender TEXT not null ,
                  time TEXT not null ,
                  text            TEXT NOT NULL
                  );")
        .execute(pool).await.expect("Cannot execute sql");

    // let placeholders: String = (0..buffer_size).into_iter().map(|_| "(?, ?, ?, ?, ?, ?)").collect::<Vec<_>>().join(", ");

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
                    let group = group.clone().unwrap();
                    let subject = subject.clone().unwrap();
                    if !trim_line.is_empty() {
                        let message = Message {
                            subject,
                            is_group: group == "我的群聊" || group == "已退出的群" || group == "已退出的多人聊天",
                            message_group: group,
                            sender: sender.to_string(),
                            time: time.to_string(),
                            text: line.to_string()
                        };
                        buffer.push(message);

                        if buffer.len() == buffer_size {
                            insert_messages(&buffer, pool).await.expect("Cannot insert message");
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

    println!("Parsing complete");
    // db_thread.join();
}

const LF: u8 = '\n' as u8;

pub fn count_lines<R: io::Read>(handle: R) -> Result<usize, io::Error> {
    let mut reader = BufReader::new(handle);
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

pub async fn analyze_text<P>(path: P, pool: &AnyPool, buffer_size: &str)
    where P: AsRef<Path> {
    walk_lines(path.as_ref(), pool, usize::from_str(buffer_size).unwrap()).await;
}

fn read_lines<P>(filename: P) -> io::Result<io::Lines<io::BufReader<File>>>
    where P: AsRef<Path>, {
    let file = File::open(filename)?;
    Ok(io::BufReader::new(file).lines())
}