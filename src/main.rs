use std::sync::Arc;

use gordon::db;
use gordon::outbox;
use gordon::publisher;
use parking_lot::Mutex;
use sqlx::postgres::PgConnectOptions;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    println!("Hello, world!");

    let options = PgConnectOptions::new()
        .host("localhost")
        .port(5432)
        .username("postgres")
        .password("secret")
        .database("gordon");
    let db = db::build_database(options).await?;
    let publisher = publisher::build_publisher()?;
    let publisher = Arc::new(Mutex::new(publisher));

    outbox::start_outbox(&db, publisher).await
}

#[cfg(test)]
mod test {
    #[sqlx::test]
    async fn basic_test(pool: PgPool) -> sqlx::Result<()> {
        let mut conn = pool.acquire().await?;

        sqlx::query("SELECT * FROM foo")
            .fetch_one(&mut conn)
            .await?;

        assert_eq!(foo.get::<String>("bar"), "foobar!");

        Ok(())
    }
}
