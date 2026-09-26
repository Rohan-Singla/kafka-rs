use std::process;
use std::time::Instant;

use kafka_rust::Result;
use kafka_rust::client::{Consumer, Producer};

const USAGE: &str = "\
mini-kafka benchmark

Drives a running broker over real TCP and reports throughput and latency.
Start the broker first:  cargo run --release --bin broker

USAGE:
    bench [OPTIONS]

OPTIONS:
    --broker <ADDR>     Broker address              [default: 127.0.0.1:9092]
    --topic <NAME>      Topic to use                [default: bench]
    --messages <N>      Total messages to produce   [default: 100000]
    --producers <N>     Concurrent producers        [default: 4]
    --size <BYTES>      Payload size per message    [default: 128]
    --batch <N>         Messages per fetch          [default: 500]
    -h, --help          Print this help
";

struct Config {
    broker: String,
    topic: String,
    messages: usize,
    producers: usize,
    size: usize,
    batch: usize,
}

#[tokio::main]
async fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let config = match parse_args(&args) {
        Ok(Some(config)) => config,
        Ok(None) => {
            print!("{}", USAGE);
            return;
        }
        Err(e) => {
            eprintln!("error: {}\n\n{}", e, USAGE);
            process::exit(2);
        }
    };

    if let Err(e) = run(config).await {
        eprintln!("benchmark failed: {}", e);
        eprintln!("is the broker running?");
        process::exit(1);
    }
}

async fn run(config: Config) -> Result<()> {
    let partitions = config.producers as u32;
    let per_producer = config.messages / config.producers;
    let total = per_producer * config.producers;

    let topic = format!("{}-p{}", config.topic, config.producers);

    println!("mini-kafka benchmark");
    println!("  broker      {}", config.broker);
    println!("  topic       {}", topic);
    println!("  messages    {}", total);
    println!("  producers   {} (one partition each)", config.producers);
    println!("  payload     {} bytes", config.size);
    println!();

    let mut setup = Producer::connect(&config.broker).await?;
    setup.create_topic(&topic, partitions).await?;

    let described = setup.describe_topic(&topic).await?;
    if described.partitions.len() as u32 != partitions {
        return Err(kafka_rust::Error::Protocol(format!(
            "topic '{}' has {} partition(s) but this run needs {}. Use --topic with a fresh name.",
            topic,
            described.partitions.len(),
            partitions
        )));
    }
    let existing = described
        .partitions
        .iter()
        .map(|p| p.next_offset)
        .collect::<Vec<u64>>();

    let payload = "x".repeat(config.size);

    let mut handles = Vec::with_capacity(config.producers);
    let start = Instant::now();

    for id in 0..config.producers {
        let addr = config.broker.clone();
        let topic = topic.clone();
        let payload = payload.clone();

        handles.push(tokio::spawn(async move {
            let mut producer = Producer::connect(&addr).await?;
            let mut latencies = Vec::with_capacity(per_producer);

            for _ in 0..per_producer {
                let sent = Instant::now();
                producer.send_to(&topic, id as u32, &payload).await?;
                latencies.push(sent.elapsed().as_micros() as u64);
            }
            Ok::<Vec<u64>, kafka_rust::Error>(latencies)
        }));
    }

    let mut latencies = Vec::with_capacity(total);
    for handle in handles {
        match handle.await {
            Ok(Ok(mut batch)) => latencies.append(&mut batch),
            Ok(Err(e)) => return Err(e),
            Err(e) => {
                return Err(kafka_rust::Error::Protocol(format!(
                    "producer task panicked: {}",
                    e
                )));
            }
        }
    }
    let produce_elapsed = start.elapsed();

    latencies.sort_unstable();
    let bytes = (total * config.size) as f64;

    println!("PRODUCE");
    report_throughput(total, bytes, produce_elapsed.as_secs_f64());
    report_latency(&latencies);
    println!();

    let consume_start = Instant::now();
    let mut consumed = 0usize;
    let mut fetch_latencies = Vec::new();
    let mut consumer = Consumer::connect(&config.broker).await?;

    for partition in 0..partitions {
        let mut offset = existing.get(partition as usize).copied().unwrap_or(0);
        loop {
            let fetched = Instant::now();
            let records = consumer
                .poll_partition(&topic, partition, offset, config.batch)
                .await?;
            fetch_latencies.push(fetched.elapsed().as_micros() as u64);

            if records.is_empty() {
                break;
            }
            offset = records[records.len() - 1].offset + 1;
            consumed += records.len();
        }
    }
    let consume_elapsed = consume_start.elapsed();
    fetch_latencies.sort_unstable();

    println!(
        "CONSUME  ({} messages read back in batches of {})",
        consumed, config.batch
    );
    report_throughput(
        consumed,
        (consumed * config.size) as f64,
        consume_elapsed.as_secs_f64(),
    );
    println!("  fetch call latency");
    report_latency(&fetch_latencies);

    if consumed != total {
        println!();
        println!("WARNING: produced {} but read back {}", total, consumed);
    }
    Ok(())
}

fn report_throughput(count: usize, bytes: f64, seconds: f64) {
    if seconds <= 0.0 {
        return;
    }
    println!(
        "  {:>12.0} msgs/sec   {:>8.2} MB/sec   over {:.2}s",
        count as f64 / seconds,
        bytes / seconds / (1024.0 * 1024.0),
        seconds
    );
}

fn report_latency(sorted: &[u64]) {
    if sorted.is_empty() {
        return;
    }
    println!(
        "  p50 {:>8}   p95 {:>8}   p99 {:>8}   max {:>8}   (microseconds)",
        percentile(sorted, 50.0),
        percentile(sorted, 95.0),
        percentile(sorted, 99.0),
        sorted[sorted.len() - 1]
    );
}

fn percentile(sorted: &[u64], p: f64) -> u64 {
    if sorted.is_empty() {
        return 0;
    }
    let rank = (p / 100.0 * (sorted.len() - 1) as f64).round() as usize;
    sorted[rank.min(sorted.len() - 1)]
}

fn parse_args(args: &[String]) -> std::result::Result<Option<Config>, String> {
    let mut config = Config {
        broker: "127.0.0.1:9092".to_string(),
        topic: "bench".to_string(),
        messages: 100_000,
        producers: 4,
        size: 128,
        batch: 500,
    };
    let mut i = 0;

    while i < args.len() {
        match args[i].as_str() {
            "-h" | "--help" => return Ok(None),
            "--broker" => config.broker = take(args, &mut i, "--broker")?,
            "--topic" => config.topic = take(args, &mut i, "--topic")?,
            "--messages" => config.messages = take_usize(args, &mut i, "--messages")?,
            "--producers" => config.producers = take_usize(args, &mut i, "--producers")?,
            "--size" => config.size = take_usize(args, &mut i, "--size")?,
            "--batch" => config.batch = take_usize(args, &mut i, "--batch")?,
            other => return Err(format!("unknown option '{}'", other)),
        }
        i += 1;
    }

    if config.producers == 0 {
        return Err("--producers must be at least 1".to_string());
    }
    if config.messages < config.producers {
        return Err("--messages must be at least --producers".to_string());
    }
    if config.batch == 0 {
        return Err("--batch must be at least 1".to_string());
    }
    Ok(Some(config))
}

fn take(args: &[String], i: &mut usize, flag: &str) -> std::result::Result<String, String> {
    *i += 1;
    args.get(*i)
        .cloned()
        .ok_or_else(|| format!("{} needs a value", flag))
}

fn take_usize(args: &[String], i: &mut usize, flag: &str) -> std::result::Result<usize, String> {
    take(args, i, flag)?
        .parse()
        .map_err(|_| format!("{} expects a number", flag))
}
