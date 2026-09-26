use kafka_rust::client::Producer;

#[tokio::main]
async fn main() -> kafka_rust::Result<()> {
    let mut producer = Producer::connect("127.0.0.1:9092").await?;

    producer.create_topic("orders", 3).await?;
    println!("topic 'orders' ready with 3 partitions\n");

    for i in 1..=9 {
        let message = format!("order #{}", i);
        let offset = producer.send("orders", &message).await?;
        println!("{:<12} -> offset {}", message, offset);
    }

    println!();
    for status in ["created", "paid", "shipped"] {
        let message = format!("order #42 {}", status);
        let offset = producer.send_to("orders", 0, &message).await?;
        println!("{:<20} -> partition 0, offset {}", message, offset);
    }

    println!("\ntopic state:");
    for partition in producer.describe_topic("orders").await?.partitions {
        println!(
            "  partition {} holds {} message(s), {} bytes",
            partition.partition,
            partition.next_offset - partition.start_offset,
            partition.size_bytes
        );
    }

    Ok(())
}
