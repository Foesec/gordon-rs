use chrono::{DateTime, Utc};
use sqlx::postgres::PgConnectOptions;
use sqlx::{postgres::PgPoolOptions, Pool, Postgres};

#[derive(sqlx::FromRow)]
pub struct TxRecord {
    pub id: i32,
    pub topic: String,
    pub key: Option<Vec<u8>>,
    pub value: Vec<u8>,
    pub published_ts: Option<DateTime<Utc>>,
}

static FETCH_QUERY: &str =
    "SELECT * FROM dev.transactional_outbox WHERE published_ts IS NULL LIMIT $1";

static WRITEBACK_PUBLISHED_QUERY: &str = "
UPDATE dev.transactional_outbox
SET published_ts = NOW()
WHERE id = ANY($1)
";

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

    pub async fn write_back_published_records(&self, ids: Vec<i32>) -> Result<u64, sqlx::Error> {
        let query_result = sqlx::query(WRITEBACK_PUBLISHED_QUERY)
            .bind(&ids)
            .execute(&self.pool)
            .await;
        query_result.map(|pg_res| pg_res.rows_affected())
    }

    pub async fn write_back_published_record(
        &self,
        id: i32,
        pub_ts: DateTime<Utc>,
    ) -> Result<(), sqlx::Error> {
        sqlx::query(
            "
UPDATE dev.transactional_outbox
SET published_ts = $1
WHERE id = $2
",
        )
        .bind(pub_ts)
        .bind(id)
        .execute(&self.pool)
        .await?;
        Ok(())
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
                ('hell', '',    'hey there',   NULL),
                ('hell', 'a',   'bye there',   NULL),
                ('nurg', 'xyz', 'there',       NULL),
                ('blrb', 'abc', 'here',        NULL),
                ('blrb', 'abc', 'nowhere',     NULL),
                ('BUPP', NULL,  'valuevalue', '2022-12-03T09:39:52+00:00')",
        )
        .execute(xct)
        .await;
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
    async fn test_db_fetch_records_to_publish(pool: PgPool) -> sqlx::Result<()> {
        prefill_value(&pool).await;

        let db = Db { pool };

        let res = db.fetch_records_to_publish(5).await?;

        assert!(res.len() <= 5);

        Ok(())
    }
}
