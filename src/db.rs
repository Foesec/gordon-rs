use chrono::{DateTime, Utc};
use once_cell::sync::OnceCell;
use sqlx::database::HasArguments;
use sqlx::prelude::*;
use sqlx::query::Query;
use sqlx::{postgres::PgPoolOptions, Executor, Pool, Postgres};
use kafka::producer::Record;

#[derive(sqlx::FromRow)]
pub struct TxRecord {
    pub id: i64,
    pub topic: String,
    pub key: Option<Vec<u8>>,
    pub value: Vec<u8>,
    pub published_ts: Option<DateTime<Utc>>,
}

static FETCH_QUERY: &str = "
  SELECT * FROM dev.transactional_outbox
  WHERE published_ts IS NULL
  LIMIT $1";

pub async fn build_database() -> Result<Db, sqlx::Error> {
    let pool = PgPoolOptions::new()
        .max_connections(5)
        .connect("postgres://postgres@localhost:5432/gordon")
        .await?;

    Ok(Db { pool })
}

pub struct Db {
    pool: Pool<Postgres>,
}

impl Db {
    pub async fn fetch_records_to_publish<'a>(&self, n: i32) -> Result<Vec<TxRecord>, sqlx::Error> {
        sqlx::query_as::<_, TxRecord>(FETCH_QUERY)
            .bind(n)
            .fetch_all(&self.pool)
            .await
    }

    pub async fn write_back_published_records(&self, ids: Vec<i64>) -> Result<(), sqlx::Error> {
        todo!("still todo")
    }

    pub async fn write_back_published_record(
        &self,
        id: i64,
        pub_ts: DateTime<Utc>,
    ) -> Result<(), sqlx::Error> {
        todo!("still todo")
    }
}
