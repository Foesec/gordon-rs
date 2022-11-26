use std::sync::Arc;

use gordon::db;
use gordon::outbox;
use gordon::publisher;
use parking_lot::Mutex;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    println!("Hello, world!");

    let db = db::build_database().await?;
    let publisher = publisher::build_publisher()?;
    let publisher = Arc::new(Mutex::new(publisher));

    outbox::start_outbox(&db, publisher).await
}
