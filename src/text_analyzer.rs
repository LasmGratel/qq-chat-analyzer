use std::{fs, io};
use std::path::Path;
use std::fs::File;
use std::io::BufRead;
use regex::Regex;
use futures::executor::{ThreadPool, block_on};
use futures::prelude::*;
use futures::task::SpawnExt;
use futures::future::RemoteHandle;
use std::sync::{Arc};
use indicatif::{ProgressBar, ProgressIterator};
use futures::channel::mpsc;
use anyhow::Error;
use uuid::Uuid;
use crate::message::{Messages, Message};
use std::cmp::max;

fn read_messages(group: String, subject: String, lines: &Vec<String>) -> Messages {
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

pub fn analyze_text<P>(path: P)
    where P: AsRef<Path> {
    let threads = max(1, num_cpus::get());
    let pool = ThreadPool::new().expect("Failed to build pool");

    let (tx, rx) = mpsc::unbounded::<anyhow::Result<usize>>();
    let mut group: Option<String> = None;
    let mut subject: Option<String> = None;
    let lines: Vec<Result<String, io::Error>> = read_lines(path).unwrap().collect();

    let mut lines_slice = vec![];
    let message_groups = async {
        let mut message_jobs: Vec<RemoteHandle<Messages>> = vec![];

        for (i, line) in lines.into_iter().enumerate() {
            let line = line.unwrap();
            let trim_line = line.trim();
            if i == 0 || i == 1 {
                continue; // Skip
            }
            if trim_line.starts_with("消息分组:") {
                if group.is_some() && subject.is_some() {
                    let cloned_slice = lines_slice.clone();
                    let group = group.clone().unwrap();
                    let subject = subject.clone().unwrap();
                    let messages_job = async move {
                        read_messages(group, subject, &cloned_slice)
                    };
                    message_jobs.push(pool.spawn_with_handle(messages_job).unwrap());
                }
                group = Some(trim_line.to_string());
                subject = None;
                lines_slice.clear();
            } else if trim_line.starts_with("消息对象:") {
                subject = Some(trim_line.to_string());
            } else {
                lines_slice.push(line);
            }
        }

        println!("Running {} jobs on {} threads", message_jobs.len(), threads);

        futures::future::join_all(message_jobs).await
    };

    let message_groups: Vec<Messages> = block_on(message_groups);
    let message_groups: Vec<Arc<Messages>> = message_groups.into_iter().map(|x| Arc::new(x)).collect();
    let count = message_groups.len();

    let bar2 = ProgressBar::new(count as u64);
    let bar = bar2.clone();

    let file_job = async move {
        if Path::new("messages").is_dir() {
            println!("Purged old messages");
            fs::remove_dir_all("messages");
        }
        fs::create_dir("messages");
        let mut file_jobs = vec![];
        for messages in message_groups.iter() {
            let messages = messages.clone();
            let bar = bar.clone();
            file_jobs.push(pool.spawn_with_handle(async move {
                let json_result = serde_json::to_string(&messages);
                if let Ok(json) = json_result {
                    let uuid = Uuid::new_v4().to_string();
                    let result = fs::write(format!("messages/{}.json", uuid), &json);
                    if let Err(e) = result {
                        eprintln!("{:?}", e);
                    } else {
                        bar.inc(1);
                    }
                } else {
                    eprintln!("{:?}", json_result.unwrap_err());
                }
            }).unwrap());
        }

        futures::future::join_all(file_jobs).await
    };

    let file_job_result = block_on(file_job);
    bar2.finish();
    println!("Written {} message files", file_job_result.len());
}

fn read_lines<P>(filename: P) -> io::Result<io::Lines<io::BufReader<File>>>
    where P: AsRef<Path>, {
    let file = File::open(filename)?;
    Ok(io::BufReader::new(file).lines())
}