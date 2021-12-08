pub mod message;
pub mod text_analyzer;
pub mod user_processor;
pub mod message_analyzer;
pub mod user;
pub mod sql_util;

extern crate serde;
extern crate serde_json;
extern crate futures;
extern crate indicatif;



use clap::{Arg, App};
use sqlx::{AnyPool, Connection, Pool, SqlitePool};
use sqlx::any::AnyPoolOptions;
use crate::message_analyzer::analyze_messages;
use crate::text_analyzer::analyze_text;
use crate::user_processor::walk_messages;

#[async_std::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let matches = App::new("QQ Chat Analyzer")
        .version("0.2.0")
        .author("Lasm Gratel <lasm_gratel@hotmail.com>")
        .about("分析QQ的纯文本消息记录")
        .arg(Arg::new("cache")
            .short('c')
            .about("插入数据库的信息缓存")
            .default_value("512")
            .required(false))
        .arg(Arg::new("INPUT")
            .about("QQ导出的全部消息记录.txt")
            .required(true)
            .index(1))
        .get_matches();
    if let Some(i) = matches.value_of("INPUT") {
        let conn_str =
            std::env::var("DATABASE_URL").expect("Env var DATABASE_URL is required for this example.");
        let mut conn = sqlx::AnyConnection::connect(&conn_str).await?;
        let pool: AnyPool = AnyPoolOptions::new().max_connections(num_cpus::get() as u32 + 4).connect(&conn_str).await?;

        println!("Step 1: Split all messages");
        analyze_text(i, pool.clone(), matches.value_of("cache").unwrap()).await?;

        println!("Step 2: Post-processing of users");
        walk_messages(pool.clone()).await?;

        println!("Step 3: Rewrite messages");
        analyze_messages(pool.clone()).await?;
    } else {
        panic!("需要指定输入文件");
    }

    Ok(())

}