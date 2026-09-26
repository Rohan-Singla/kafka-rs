use std::collections::HashSet;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use kafka_rust::Error;
use kafka_rust::broker::{Broker, BrokerConfig};
use kafka_rust::client::{Consumer, Producer};
use kafka_rust::network::Server;
use kafka_rust::network::codec::MAX_FRAME_SIZE;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::task::JoinHandle;

struct RunningBroker {
    addr: String,
    handle: JoinHandle<()>,
}

impl RunningBroker {
    async fn start(data_dir: PathBuf) -> Self {
        let mut config = BrokerConfig::new(data_dir);
        config.session_timeout = Duration::from_secs(60);

        let broker = Arc::new(Broker::open(config).expect("broker should open"));
        let server = Server::bind(broker, "127.0.0.1:0")
            .await
            .expect("bind should succeed");
        let addr = server.local_addr().unwrap().to_string();

        let handle = tokio::spawn(async move {
            let _ = server.run().await;
        });
        Self { addr, handle }
    }

    async fn stop(self) {
        self.handle.abort();
        let _ = self.handle.await;
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

#[tokio::test]
async fn produce_and_consume_over_tcp() {
    let dir = tempfile::tempdir().unwrap();
    let broker = RunningBroker::start(dir.path().to_path_buf()).await;

    let mut producer = Producer::connect(&broker.addr).await.unwrap();
    producer.create_topic("orders", 1).await.unwrap();

    for i in 0..5 {
        let offset = producer
            .send_to("orders", 0, &format!("order {}", i))
            .await
            .unwrap();
        assert_eq!(offset, i);
    }

    let mut consumer = Consumer::connect(&broker.addr).await.unwrap();
    let records = consumer.poll_partition("orders", 0, 0, 100).await.unwrap();

    assert_eq!(records.len(), 5);
    assert_eq!(records[0].value, "order 0");
    assert_eq!(records[4].value, "order 4");
    assert!(records[0].timestamp > 0);

    broker.stop().await;
}

#[tokio::test]
async fn round_robin_spreads_across_partitions() {
    let dir = tempfile::tempdir().unwrap();
    let broker = RunningBroker::start(dir.path().to_path_buf()).await;

    let mut producer = Producer::connect(&broker.addr).await.unwrap();
    producer.create_topic("events", 3).await.unwrap();
    for i in 0..9 {
        producer
            .send("events", &format!("event {}", i))
            .await
            .unwrap();
    }

    let info = producer.describe_topic("events").await.unwrap();
    for partition in &info.partitions {
        assert_eq!(
            partition.next_offset, 3,
            "partition {} should hold 3 of the 9 messages",
            partition.partition
        );
    }

    broker.stop().await;
}

#[tokio::test]
async fn a_consumer_group_splits_partitions_without_double_reading() {
    let dir = tempfile::tempdir().unwrap();
    let broker = RunningBroker::start(dir.path().to_path_buf()).await;

    let mut producer = Producer::connect(&broker.addr).await.unwrap();
    producer.create_topic("orders", 4).await.unwrap();
    for i in 0..40 {
        producer
            .send("orders", &format!("order {}", i))
            .await
            .unwrap();
    }

    let mut first = Consumer::connect(&broker.addr).await.unwrap();
    let mut second = Consumer::connect(&broker.addr).await.unwrap();
    first.subscribe("processors", &["orders"]).await.unwrap();
    second.subscribe("processors", &["orders"]).await.unwrap();

    let mut seen: Vec<String> = Vec::new();
    for consumer in [&mut first, &mut second] {
        loop {
            let records = consumer.poll(100).await.unwrap();
            if records.is_empty() {
                break;
            }
            seen.extend(records.into_iter().map(|r| r.value));
        }
    }

    assert_eq!(seen.len(), 40, "every message should be read exactly once");
    let unique: HashSet<&String> = seen.iter().collect();
    assert_eq!(unique.len(), 40, "no message should be read twice");

    assert_eq!(first.assignment().len(), 2);
    assert_eq!(second.assignment().len(), 2);

    broker.stop().await;
}

#[tokio::test]
async fn leaving_a_group_rebalances_onto_the_survivor() {
    let dir = tempfile::tempdir().unwrap();
    let broker = RunningBroker::start(dir.path().to_path_buf()).await;

    let mut producer = Producer::connect(&broker.addr).await.unwrap();
    producer.create_topic("orders", 4).await.unwrap();
    producer.send("orders", "seed").await.unwrap();

    let mut staying = Consumer::connect(&broker.addr).await.unwrap();
    let mut leaving = Consumer::connect(&broker.addr).await.unwrap();
    staying.subscribe("g", &["orders"]).await.unwrap();
    leaving.subscribe("g", &["orders"]).await.unwrap();

    staying.poll(10).await.unwrap();
    assert_eq!(staying.assignment().len(), 2);

    leaving.leave().await.unwrap();

    staying.poll(10).await.unwrap();
    assert_eq!(staying.assignment().len(), 4);

    broker.stop().await;
}

#[tokio::test]
async fn more_consumers_than_partitions_leaves_spares_idle() {
    let dir = tempfile::tempdir().unwrap();
    let broker = RunningBroker::start(dir.path().to_path_buf()).await;

    let mut producer = Producer::connect(&broker.addr).await.unwrap();
    producer.create_topic("small", 1).await.unwrap();
    producer.send("small", "only message").await.unwrap();

    let mut consumers = Vec::new();
    for _ in 0..3 {
        let mut consumer = Consumer::connect(&broker.addr).await.unwrap();
        consumer.subscribe("g", &["small"]).await.unwrap();
        consumers.push(consumer);
    }

    let mut total = 0;
    let mut with_work = 0;
    for consumer in &mut consumers {
        let records = consumer.poll(10).await.unwrap();
        total += records.len();
        if !consumer.assignment().is_empty() {
            with_work += 1;
        }
    }

    assert_eq!(total, 1);
    assert_eq!(
        with_work, 1,
        "only one consumer can own the single partition"
    );

    broker.stop().await;
}

#[tokio::test]
async fn messages_and_committed_offsets_survive_a_restart() {
    let dir = tempfile::tempdir().unwrap();
    let data_dir = dir.path().to_path_buf();

    let broker = RunningBroker::start(data_dir.clone()).await;
    {
        let mut producer = Producer::connect(&broker.addr).await.unwrap();
        producer.create_topic("orders", 1).await.unwrap();
        for i in 0..10 {
            producer
                .send_to("orders", 0, &format!("order {}", i))
                .await
                .unwrap();
        }

        let mut consumer = Consumer::connect(&broker.addr).await.unwrap();
        consumer.subscribe("durable", &["orders"]).await.unwrap();

        let records = consumer.poll(4).await.unwrap();
        assert_eq!(records.len(), 4);
        consumer.commit().await.unwrap();
    }
    broker.stop().await;

    let broker = RunningBroker::start(data_dir).await;

    let mut consumer = Consumer::connect(&broker.addr).await.unwrap();
    consumer.subscribe("durable", &["orders"]).await.unwrap();

    let records = consumer.poll(100).await.unwrap();
    assert_eq!(records.len(), 6);
    assert_eq!(records[0].offset, 4);
    assert_eq!(records[0].value, "order 4");

    let mut producer = Producer::connect(&broker.addr).await.unwrap();
    assert_eq!(
        producer
            .send_to("orders", 0, "after restart")
            .await
            .unwrap(),
        10
    );

    let mut replayer = Consumer::connect(&broker.addr).await.unwrap();
    let all = replayer.poll_partition("orders", 0, 0, 100).await.unwrap();
    assert_eq!(all.len(), 11);
    assert_eq!(all[0].value, "order 0");

    broker.stop().await;
}

#[tokio::test]
async fn topics_are_rediscovered_after_a_restart() {
    let dir = tempfile::tempdir().unwrap();
    let data_dir = dir.path().to_path_buf();

    let broker = RunningBroker::start(data_dir.clone()).await;
    {
        let mut producer = Producer::connect(&broker.addr).await.unwrap();
        producer.create_topic("alpha", 2).await.unwrap();
        producer.create_topic("beta", 1).await.unwrap();
        producer.send_to("alpha", 1, "hello").await.unwrap();
    }
    broker.stop().await;

    let broker = RunningBroker::start(data_dir).await;
    let mut producer = Producer::connect(&broker.addr).await.unwrap();

    let topics = producer.list_topics().await.unwrap();
    let names: Vec<&str> = topics.iter().map(|t| t.name.as_str()).collect();
    assert!(names.contains(&"alpha"), "got {:?}", names);
    assert!(names.contains(&"beta"), "got {:?}", names);

    let alpha = producer.describe_topic("alpha").await.unwrap();
    assert_eq!(alpha.partitions.len(), 2);
    assert_eq!(alpha.partitions[1].next_offset, 1);

    broker.stop().await;
}

#[tokio::test]
async fn concurrent_producers_do_not_lose_or_duplicate_offsets() {
    let dir = tempfile::tempdir().unwrap();
    let broker = RunningBroker::start(dir.path().to_path_buf()).await;

    let mut setup = Producer::connect(&broker.addr).await.unwrap();
    setup.create_topic("hot", 1).await.unwrap();

    let mut handles = Vec::new();
    for id in 0..8 {
        let addr = broker.addr.clone();
        handles.push(tokio::spawn(async move {
            let mut producer = Producer::connect(&addr).await.unwrap();
            let mut offsets = Vec::new();
            for i in 0..50 {
                offsets.push(
                    producer
                        .send_to("hot", 0, &format!("{}-{}", id, i))
                        .await
                        .unwrap(),
                );
            }
            offsets
        }));
    }

    let mut all = Vec::new();
    for handle in handles {
        all.extend(handle.await.unwrap());
    }

    all.sort_unstable();
    let expected: Vec<u64> = (0..400).collect();
    assert_eq!(all, expected, "every offset handed out exactly once");

    let mut consumer = Consumer::connect(&broker.addr).await.unwrap();
    let records = consumer.poll_partition("hot", 0, 0, 1000).await.unwrap();
    assert_eq!(records.len(), 400);

    broker.stop().await;
}

#[tokio::test]
async fn errors_come_back_without_killing_the_connection() {
    let dir = tempfile::tempdir().unwrap();
    let broker = RunningBroker::start(dir.path().to_path_buf()).await;

    let mut producer = Producer::connect(&broker.addr).await.unwrap();

    let rejected = producer.send_to("ghost", 0, "x").await;
    assert!(matches!(rejected, Err(Error::Broker(_))));
    assert_eq!(
        rejected.unwrap_err().to_string(),
        "unknown topic 'ghost'",
        "a broker error must not be re-prefixed by the client"
    );

    assert!(producer.create_topic("../../etc/passwd", 1).await.is_err());

    producer.create_topic("real", 1).await.unwrap();
    assert!(producer.send_to("real", 9, "x").await.is_err());

    assert_eq!(producer.send_to("real", 0, "still here").await.unwrap(), 0);

    broker.stop().await;
}

#[tokio::test]
async fn an_oversized_frame_is_refused() {
    let dir = tempfile::tempdir().unwrap();
    let broker = RunningBroker::start(dir.path().to_path_buf()).await;

    let mut socket = tokio::net::TcpStream::connect(&broker.addr).await.unwrap();
    socket.write_all(&u32::MAX.to_be_bytes()).await.unwrap();
    socket.flush().await.unwrap();

    let mut length_buf = [0u8; 4];
    socket.read_exact(&mut length_buf).await.unwrap();
    let length = u32::from_be_bytes(length_buf) as usize;
    assert!(length < MAX_FRAME_SIZE);

    let mut body = vec![0u8; length];
    socket.read_exact(&mut body).await.unwrap();
    let text = String::from_utf8_lossy(&body);
    assert!(text.contains("exceeds"), "got {}", text);

    let mut producer = Producer::connect(&broker.addr).await.unwrap();
    producer.create_topic("alive", 1).await.unwrap();

    broker.stop().await;
}

#[tokio::test]
async fn malformed_json_is_an_error_not_a_crash() {
    let dir = tempfile::tempdir().unwrap();
    let broker = RunningBroker::start(dir.path().to_path_buf()).await;

    let mut socket = tokio::net::TcpStream::connect(&broker.addr).await.unwrap();
    let garbage = b"{not json at all";
    socket
        .write_all(&(garbage.len() as u32).to_be_bytes())
        .await
        .unwrap();
    socket.write_all(garbage).await.unwrap();

    let mut length_buf = [0u8; 4];
    socket.read_exact(&mut length_buf).await.unwrap();
    let mut body = vec![0u8; u32::from_be_bytes(length_buf) as usize];
    socket.read_exact(&mut body).await.unwrap();
    assert!(String::from_utf8_lossy(&body).contains("malformed"));

    let valid = br#"{"type":"ListTopics"}"#;
    socket
        .write_all(&(valid.len() as u32).to_be_bytes())
        .await
        .unwrap();
    socket.write_all(valid).await.unwrap();

    socket.read_exact(&mut length_buf).await.unwrap();
    let mut body = vec![0u8; u32::from_be_bytes(length_buf) as usize];
    socket.read_exact(&mut body).await.unwrap();
    assert!(String::from_utf8_lossy(&body).contains("Topics"));

    broker.stop().await;
}

#[tokio::test]
async fn group_admin_views_reflect_reality() {
    let dir = tempfile::tempdir().unwrap();
    let broker = RunningBroker::start(dir.path().to_path_buf()).await;

    let mut producer = Producer::connect(&broker.addr).await.unwrap();
    producer.create_topic("orders", 2).await.unwrap();
    producer.send("orders", "one").await.unwrap();

    let mut consumer = Consumer::connect(&broker.addr).await.unwrap();
    consumer.subscribe("watchers", &["orders"]).await.unwrap();
    consumer.poll(10).await.unwrap();
    consumer.commit().await.unwrap();

    let groups = consumer.list_groups().await.unwrap();
    assert!(
        groups
            .iter()
            .any(|g| g.group == "watchers" && g.members == 1)
    );

    let (description, committed) = consumer.describe_group("watchers").await.unwrap();
    assert_eq!(description.members.len(), 1);
    assert_eq!(description.members[0].partitions.len(), 2);
    assert!(!committed.is_empty());

    broker.stop().await;
}

#[tokio::test]
async fn a_huge_fetch_is_capped_instead_of_dropping_the_connection() {
    let dir = tempfile::tempdir().unwrap();
    let broker = RunningBroker::start(dir.path().to_path_buf()).await;

    let mut producer = Producer::connect(&broker.addr).await.unwrap();
    producer.create_topic("big", 1).await.unwrap();

    let payload = "x".repeat(1024 * 1024);
    for _ in 0..20 {
        producer.send_to("big", 0, &payload).await.unwrap();
    }

    let mut consumer = Consumer::connect(&broker.addr).await.unwrap();
    let first = consumer.poll_partition("big", 0, 0, 20).await.unwrap();

    assert!(!first.is_empty(), "a fetch must never stall");
    assert!(
        first.len() < 20,
        "20MB of messages came back in one frame, got {} records",
        first.len()
    );

    let next = consumer
        .poll_partition("big", 0, first.len() as u64, 20)
        .await
        .unwrap();
    assert!(!next.is_empty(), "the connection survived and kept serving");

    broker.stop().await;
}

#[tokio::test]
async fn payloads_that_expand_under_json_escaping_still_get_delivered() {
    let dir = tempfile::tempdir().unwrap();
    let broker = RunningBroker::start(dir.path().to_path_buf()).await;

    let mut producer = Producer::connect(&broker.addr).await.unwrap();
    producer.create_topic("escaped", 1).await.unwrap();

    let payload = "\u{1}".repeat(1024 * 1024);
    for _ in 0..4 {
        producer.send_to("escaped", 0, &payload).await.unwrap();
    }

    let mut consumer = Consumer::connect(&broker.addr).await.unwrap();
    let records = consumer.poll_partition("escaped", 0, 0, 500).await.unwrap();

    assert!(
        !records.is_empty(),
        "a batch inside the on-disk budget became undeliverable once escaped"
    );
    assert_eq!(records[0].value.len(), payload.len());
    assert_eq!(records[0].value, payload);

    broker.stop().await;
}

#[tokio::test]
async fn recreating_a_topic_with_a_different_partition_count_is_refused() {
    let dir = tempfile::tempdir().unwrap();
    let broker = RunningBroker::start(dir.path().to_path_buf()).await;

    let mut producer = Producer::connect(&broker.addr).await.unwrap();
    producer.create_topic("orders", 2).await.unwrap();

    assert!(
        producer.create_topic("orders", 8).await.is_err(),
        "a silent no-op here makes callers produce to partitions that do not exist"
    );
    assert_eq!(
        producer
            .describe_topic("orders")
            .await
            .unwrap()
            .partitions
            .len(),
        2
    );

    broker.stop().await;
}

#[tokio::test]
async fn a_consumer_must_subscribe_before_polling() {
    let dir = tempfile::tempdir().unwrap();
    let broker = RunningBroker::start(dir.path().to_path_buf()).await;

    let mut consumer = Consumer::connect(&broker.addr).await.unwrap();
    assert!(matches!(consumer.poll(10).await, Err(Error::Protocol(_))));
    assert!(matches!(consumer.commit().await, Err(Error::Protocol(_))));

    broker.stop().await;
}
