use futures::executor::ThreadPool;
use futures::future::join_all;
use futures::task::SpawnExt;
use sqlx::{AnyConnection, AnyPool, Error, Row};

use crate::sql_util::count;
use indicatif::MultiProgress;

async fn modify_messages(conn: AnyPool) -> Result<(), Error> {
    let nicks = sqlx::query("SELECT value, user_id FROM nicks")
        .fetch_all(&conn)
        .await?;
    let multi_bar = indicatif::MultiProgress::new();
    let nick_bar = indicatif::ProgressBar::new(nicks.len() as u64);
    let rows_bar = indicatif::ProgressBar::new(count("messages where user_id IS NULL", &conn).await? as u64);

    let mut jobs = vec![];
    let pool = ThreadPool::builder().create().unwrap();

    for row in nicks.into_iter() {
        let nick_bar = nick_bar.clone();
        let rows_bar = rows_bar.clone();
        let conn = conn.clone();
        jobs.push(pool.spawn_with_handle(async move {
            let value = row.get::<String, usize>(0);
            let user_id = row.get::<i64, usize>(1);
            nick_bar.inc(1);
            rows_bar.inc(sqlx::query("UPDATE messages SET user_id = ? WHERE sender like ? AND user_id IS NULL")
                .bind(user_id)
                .bind(value + "%")
                .execute(&conn)
                .await
                .expect("Cannot execute SQL").rows_affected() as u64);
        }).expect("Cannot spawn"));
    };
    join_all(jobs.into_iter()).await;
    rows_bar.finish();
    nick_bar.finish();
    Ok(())
}

pub async fn analyze_messages(conn: AnyPool) -> Result<(), Error> {
    sqlx::query("
ALTER TABLE messages ADD COLUMN user_id INTEGER;
")
    .execute(&conn)
    .await;
    /*sqlx::query("\
VACUUM;
PRAGMA journal_mode = WAL;
PRAGMA synchronous = NORMAL;
PRAGMA page_size = 4096;
PRAGMA busy_timeout=60000;
PRAGMA foreign_keys = ON;
")
    .execute(&conn)
    .await?;*/

    modify_messages(conn).await?;
    Ok(())
}