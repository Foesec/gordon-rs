use std::time::Duration;

use kafka::producer::{AsBytes, Producer, Record, RequiredAcks};

use crate::db::TxRecord;

pub fn build_publisher() -> anyhow::Result<SimplePublisher> {
    let producer = Producer::from_hosts(vec!["localhost:9092".to_owned()])
        .with_ack_timeout(Duration::from_secs(1))
        .with_required_acks(RequiredAcks::One)
        .create()?;

    Ok(SimplePublisher { producer })
}

pub trait OutboxPublisher {
    async fn publish_record_sync(&mut self, rec: &TxRecord) -> Result<(), anyhow::Error>;
}

pub struct SimplePublisher {
    pub producer: Producer,
}

impl SimplePublisher {
    pub fn publish_record_sync(&mut self, rec: &TxRecord) -> kafka::Result<()> {
        self.producer.send(&rec.into())
    }
}

impl<'a> From<&'a TxRecord> for Record<'a, &'a[u8], &'a[u8]> {
    fn from(txr: &'a TxRecord) -> Self {
        match txr.key.as_ref() {
            Some(key) => Record::from_key_value(txr.topic.as_str(), key.as_bytes(), txr.value.as_bytes()),
            None => Record::from_key_value(txr.topic.as_str(), ().as_bytes(), txr.value.as_bytes()),
        }
    }
}
