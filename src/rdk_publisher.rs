use std::time::Duration;

use rdkafka::{
    config::ClientConfig,
    producer::{FutureProducer, FutureRecord},
};

use crate::{db::TxRecord, publisher::OutboxPublisher};

pub fn build_publisher() -> anyhow::Result<RdkPublisher> {
    let client: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", "localhost:9092")
        .set("queue.buffering.max.ms", "0")
        .set("auto.create.topics.enable", "true")
        .create()?;

    Ok(RdkPublisher { client })
}

pub struct RdkPublisher {
    client: FutureProducer,
}

impl OutboxPublisher for RdkPublisher {
    async fn publish_record_sync(&mut self, rec: &TxRecord) -> Result<(), anyhow::Error> {
        let mut record = FutureRecord::to(&rec.topic);
        if let Some(key) = &rec.key {
            record = record.key(key)
        };
        record = record.payload(&rec.value);

        let send_result = self.client.send(record, Duration::from_secs(1)).await;

        send_result
            .map(|(_partition, _offset)| ())
            .map_err(|(err, _msg)| err.into())
    }
}
