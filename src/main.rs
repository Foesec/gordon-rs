use std::sync::Arc;

use parking_lot::Mutex;

mod db;
mod publisher;
mod outbox;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    println!("Hello, world!");

    let db = db::build_database().await?;
    let publisher = publisher::build_publisher()?;
    let publisher = Arc::new(Mutex::new(publisher));

    outbox::start_outbox(&db, publisher).await
}
