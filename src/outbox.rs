use std::{
    sync::Arc,
    time::Duration,
};

use parking_lot::Mutex;
use crate::{db::Db, publisher::{SimplePublisher, OutboxPublisher}};
use chrono::Utc;
use tokio::time::sleep;

pub async fn start_outbox(db: &Db, publisher: Arc<Mutex<dyn OutboxPublisher>>) -> anyhow::Result<()> {
    loop {
        let recs = db.fetch_records_to_publish(10).await?;
        for rec in recs {
            let publisher = Arc::clone(&publisher);
            let id = rec.id;
            tokio::task::spawn_blocking(move || {
                let mut publisher = publisher.lock();
                publisher.publish_record_sync(&rec)
            })
            .await??;
            db.write_back_published_record(id, Utc::now()).await?;
        }
        sleep(Duration::from_secs(1)).await;
    }
}
