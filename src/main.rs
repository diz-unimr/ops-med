mod config;
mod fhir;

use crate::config::{Kafka, Ssl};
use crate::fhir::Mapper;
use config::AppConfig;
use futures::stream::FuturesUnordered;
use futures::{StreamExt, TryStreamExt};
use log::{debug, error, info};
use rdkafka::config::RDKafkaLogLevel;
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::message::{BorrowedMessage, Headers, Message};
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::util::Timeout;
use rdkafka::ClientConfig;
use std::env;
use std::error::Error;
use std::ops::Deref;
use std::sync::Arc;

async fn run(config: Kafka, topic: String, mapper: Mapper) {
    // create consumer
    let consumer: StreamConsumer = create_consumer(config.clone());
    match consumer.subscribe(&[&config.input_topic]) {
        Ok(_) => {
            info!("Successfully subscribed to topic: {:?}", topic);
        }
        Err(error) => error!("Failed to subscribe to specified topic: {}", error),
    }
    let consumer = Arc::new(consumer);
    let producer = Arc::new(create_producer(config.clone()));

    let stream = consumer
        .stream()
        .map_err(|e| Box::new(e) as Box<dyn Error>)
        .try_for_each(|m| {
            let consumer = consumer.clone();
            let producer = producer.clone();
            let output_topic = config.output_topic.clone();
            let mapper = mapper.clone();

            {
                async move {
                    let (key, payload) = deserialize_message(&m);

                    debug!(
                        "Message received: key: '{}', topic: {}, partition: {}, offset: {}, timestamp: {:?}",
                        key,
                        m.topic(),
                        m.partition(),
                        m.offset(),
                        m.timestamp()
                    );

                    if let Some(headers) = m.headers() {
                        for header in headers.iter() {
                            debug!("Header {:#?}: {:?}", header.key, header.value);
                        }
                    }

                    // filter tombstone records
                    if payload.is_none() {
                        return Ok(());
                    }

                    // mapper
                    let Some(result) = mapper.map(payload.unwrap())
                        .map_err(|e| error!("Failed to map payload [key={key}]: {}",e))
                        .unwrap() else { return Ok(()); };

                    // send to output topic
                    let mut record = FutureRecord::to(&output_topic)
                        .key(key.deref())
                        .payload(result.as_str());
                    record.timestamp=m.timestamp().to_millis();

                    let produce_future = producer.send(record, Timeout::Never);
                    match produce_future.await {
                        Ok(delivery) => {
                            debug!("Message sent: key: {key}, partition: {}, offset: {}", delivery.0,delivery.1);
                            // store offset
                            consumer
                                .store_offset_from_message(&m)
                                .expect("Failed to store offset for message");
                        }
                        Err((e, _)) => println!("Error: {:?}", e),
                    }

                    Ok(())
                }
            }
        });

    info!("Starting consumer");
    let error = stream.await;
    info!("Consumers terminated: {:?}", error);
}

#[tokio::main]
async fn main() {
    let config = match AppConfig::new() {
        Ok(s) => s,
        Err(e) => panic!("Failed to parse app settings: {e:?}"),
    };
    env::set_var("RUST_LOG", config.app.log_level.clone());
    env_logger::init();

    // mapper
    let mapper = Mapper::new(config.clone()).expect("failed to create mapper");

    // run
    let topic = config.kafka.input_topic.clone();
    let num_partitions = 3;
    (0..num_partitions)
        .map(|_| tokio::spawn(run(config.kafka.clone(), topic.clone(), mapper.clone())))
        .collect::<FuturesUnordered<_>>()
        .for_each(|_| async { () })
        .await
}

fn create_consumer(config: Kafka) -> StreamConsumer {
    let mut c = ClientConfig::new();
    c.set("bootstrap.servers", config.brokers)
        .set("security.protocol", config.security_protocol)
        .set("enable.partition.eof", "false")
        .set("group.id", config.consumer_group)
        .set("session.timeout.ms", "6000")
        .set("enable.auto.commit", "true")
        .set("enable.auto.offset.store", "false")
        .set("auto.offset.reset", config.offset_reset)
        .set_log_level(RDKafkaLogLevel::Debug);

    set_ssl_config(c, config.ssl)
        .create()
        .expect("Failed to create Kafka consumer")
}

fn create_producer(config: Kafka) -> FutureProducer {
    let mut c = ClientConfig::new();
    c.set("bootstrap.servers", config.brokers)
        .set("security.protocol", config.security_protocol)
        .set("compression.type", "gzip")
        .set("message.max.bytes", "6242880")
        .set_log_level(RDKafkaLogLevel::Debug);

    set_ssl_config(c, config.ssl)
        .create()
        .expect("Failed to create Kafka producer")
}

fn set_ssl_config(mut c: ClientConfig, ssl_config: Option<Ssl>) -> ClientConfig {
    if let Some(ssl) = ssl_config {
        if let Some(value) = ssl.ca_location {
            c.set("ssl.ca.location", value);
        }
        if let Some(value) = ssl.key_location {
            c.set("ssl.key.location", value);
        }
        if let Some(value) = ssl.certificate_location {
            c.set("ssl.certificate.location", value);
        }
        if let Some(value) = ssl.key_password {
            c.set("ssl.key.password", value);
        }
    }
    c
}

fn deserialize_message(m: &BorrowedMessage) -> (String, Option<String>) {
    let key = match m.key_view::<str>() {
        None => "",
        Some(Ok(k)) => k,
        Some(Err(e)) => {
            error!("Error while deserializing message key: {:?}", e);
            ""
        }
    };
    let payload = match m.payload_view::<str>() {
        None => None,
        Some(Ok(s)) => Some(s),
        Some(Err(e)) => {
            error!("Error while deserializing message payload: {:?}", e);
            None
        }
    };

    (key.to_owned(), payload.map(str::to_string).to_owned())
}

#[cfg(test)]
mod tests {
    use crate::config::AppConfig;
    use rdkafka::mocking::MockCluster;
    use rdkafka::producer::future_producer::OwnedDeliveryResult;
    use rdkafka::producer::{FutureProducer, FutureRecord};
    use std::time::{SystemTime, UNIX_EPOCH};

    #[tokio::test]
    async fn test_run() {
        const TOPIC: &str = "test_topic";

        // create mock cluster
        let mock_cluster = MockCluster::new(3).unwrap();
        mock_cluster
            .create_topic(TOPIC, 3, 3)
            .expect("Failed to create topic");

        let mock_producer: FutureProducer = rdkafka::ClientConfig::new()
            .set("bootstrap.servers", mock_cluster.bootstrap_servers())
            .create()
            .expect("Producer creation error");

        send_record(mock_producer.clone(), TOPIC, "test")
            .await
            .unwrap();
        send_record(mock_producer.clone(), TOPIC, "done")
            .await
            .unwrap();

        // setup configsubstanzangabe_aus_ops_code
        let mut config = AppConfig::default();
        config.kafka.brokers = mock_cluster.bootstrap_servers();
        config.kafka.offset_reset = String::from("earliest");
        config.kafka.security_protocol = String::from("plaintext");
        config.kafka.consumer_group = String::from("test");

        // TODO rewrite
        // run(config, TOPIC.to_string()).await;

        // mock was called once
        // post_mock.assert();
        // done.assert();
    }

    async fn send_record(
        producer: FutureProducer,
        topic: &str,
        payload: &str,
    ) -> OwnedDeliveryResult {
        producer
            .send_result(
                FutureRecord::to(topic)
                    .key("test")
                    .payload(payload)
                    .timestamp(
                        SystemTime::now()
                            .duration_since(UNIX_EPOCH)
                            .unwrap()
                            .as_millis()
                            .try_into()
                            .unwrap(),
                    ),
            )
            .unwrap()
            .await
            .unwrap()
    }
}
