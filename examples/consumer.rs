use kafka_rust::client::Consumer;

#[tokio::main]
async fn main() -> kafka_rust::Result<()> {
    let mut consumer = Consumer::connect("127.0.0.1:9092").await?;

    consumer.subscribe("order-processors", &["orders"]).await?;
    println!(
        "joined group 'order-processors' as {} (generation {})",
        consumer.member_id().unwrap_or("?"),
        consumer.generation()
    );

    let owned: Vec<String> = consumer
        .assignment()
        .iter()
        .map(|tp| format!("{}:{}", tp.topic, tp.partition))
        .collect();
    println!("assigned partitions: {}\n", owned.join(" "));

    let mut total = 0;
    loop {
        let records = consumer.poll(10).await?;
        if records.is_empty() {
            break;
        }
        for record in records {
            println!(
                "partition {} offset {:<3} {}",
                record.partition, record.offset, record.value
            );
            total += 1;
        }
    }

    if total == 0 {
        println!("no new messages, everything up to the committed offset was already read");
    } else {
        consumer.commit().await?;
        println!("\nread {} message(s) and committed", total);
    }

    consumer.leave().await?;
    Ok(())
}
