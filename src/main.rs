pub mod message;
pub mod text_analyzer;
pub mod text_processor;

extern crate serde;
extern crate serde_json;
extern crate futures;
extern crate indicatif;

use crate::text_analyzer::analyze_text;
use crate::text_processor::walk_messages;
use clap::{Arg, App};

fn main() {
    let matches = App::new("QQ Chat Analyzer")
        .version("1.0")
        .author("Lasm Gratel <lasm_gratel@hotmail.com>")
        .about("分析QQ的纯文本消息记录")
        .arg(Arg::new("cache")
            .short('c')
            .about("插入数据库的信息缓存")
            .default_value("256")
            .required(false))
        .arg(Arg::new("INPUT")
            .about("QQ导出的全部消息记录.txt")
            .required(true)
            .index(1))
        .get_matches();
    if let Some(i) = matches.value_of("INPUT") {
        println!("Step 1: Split all messages");
        analyze_text(i, matches.value_of("cache").unwrap());

        println!("Step 2: Post-processing of users");
        walk_messages("messages");

        println!("Step 3: Rewrite messages");
    } else {
        panic!("需要指定输入文件");
    }

}