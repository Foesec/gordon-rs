use chrono::{DateTime, Utc};
use sqlx::postgres::PgConnectOptions;
use sqlx::{postgres::PgPoolOptions, Pool, Postgres};

#[derive(sqlx::FromRow)]
pub struct TxRecord {
    pub id: i64,
    pub topic: String,
    pub key: Option<Vec<u8>>,
    pub value: Vec<u8>,
    pub published_ts: Option<DateTime<Utc>>,
}

static FETCH_QUERY: &str =
    "SELECT * FROM dev.transactional_outbox WHERE published_ts IS NULL LIMIT $1";

pub async fn build_database(conn_opt: PgConnectOptions) -> Result<Db, sqlx::Error> {
    let pool = PgPoolOptions::new()
        .max_connections(5)
        .connect_with(conn_opt)
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

#[cfg(test)]
mod test {
    use super::*;
    use sqlx::{pool::PoolConnection, postgres::PgQueryResult, Executor, PgPool};

    /**
     * Panics if the insert fails, for simplicity.
     */
    async fn prefill_value<'a, E>(xct: E) -> PgQueryResult
    where
        E: Executor<'a, Database = Postgres>,
    {
        let prefill = sqlx::query(
            "INSERT INTO dev.transactional_outbox(topic, key, value, published_ts)
             VALUES
                ('hell', decode('', 'hex'),    decode('hey there', 'hex'),   NULL),
                ('hell', decode('a', 'hex'),   decode('bye there', 'hex'),   NULL),
                ('nurg', decode('xyz', 'hex'), decode('there', 'hex'),       NULL),
                ('blrb', decode('abc', 'hex'), decode('here', 'hex'),        NULL),
                ('blrb', decode('abc', 'hex'), decode('nowhere', 'hex'),     NULL),
                ('BUP',  NULL,                 decode('valuevalue', 'hex'), '2022-12-03T09:39:52+00:00')",
        )
        .execute(xct).await;
        let result = prefill.unwrap();
        assert_eq!(result.rows_affected(), 6);
        result
    }

    #[sqlx::test]
    async fn test_fetch_query(mut pconn: PoolConnection<Postgres>) {
        prefill_value(&mut pconn).await;

        let result = sqlx::query_as::<_, TxRecord>(FETCH_QUERY)
            .bind(6)
            .fetch_all(&mut pconn)
            .await;
        let result = result.unwrap();

        assert!(result.len() <= 5);
    }

    #[sqlx::test]
    async fn test_db__fetch_records_to_publish(pool: PgPool) -> sqlx::Result<()> {

        prefill_value(&pool).await;

        let db = Db { pool };

        let res = db.fetch_records_to_publish(5).await?;

        Ok(())
    }
}
