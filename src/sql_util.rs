use sqlx::{AnyPool, Error, Row};

pub async fn count(sql: &str, conn: &AnyPool) -> Result<i64, Error> {
    sqlx::query(&format!("select count(*) from {}", sql))
        .fetch_one(conn)
        .await.map(|x| x.get::<i64, usize>(0))
}