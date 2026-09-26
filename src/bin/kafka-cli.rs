use std::process;

use kafka_rust::Result;
use kafka_rust::client::{Consumer, Producer};

const USAGE: &str = "\
mini-kafka admin CLI

USAGE:
    kafka-cli [--broker <ADDR>] <COMMAND>

COMMANDS:
    create-topic <name> [--partitions N]      Create a topic          [default: 1]
    list-topics                               List topics and their offsets
    describe <topic>                          Show per partition detail
    produce <topic> <message> [--partition N] Send one message (round robin by default)
    consume <topic> [--partition N] [--from N] [--max N]
                                              Read messages from a partition
    groups                                    List consumer groups
    describe-group <group>                    Show members and committed offsets

OPTIONS:
    --broker <ADDR>   Broker address  [default: 127.0.0.1:9092]
    --                Treat everything after this as positional
    -h, --help        Print this help
";

#[tokio::main]
async fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    match run(args).await {
        Ok(()) => {}
        Err(e) => {
            eprintln!("error: {}", e);
            process::exit(1);
        }
    }
}

async fn run(args: Vec<String>) -> Result<()> {
    let mut broker = "127.0.0.1:9092".to_string();
    let mut positional: Vec<String> = Vec::new();
    let mut flags: Vec<(String, String)> = Vec::new();
    let mut i = 0;

    let mut only_positional = false;

    while i < args.len() {
        let arg = &args[i];
        if only_positional {
            positional.push(arg.clone());
        } else if arg == "--" {
            only_positional = true;
        } else if arg == "-h" || arg == "--help" {
            print!("{}", USAGE);
            return Ok(());
        } else if arg == "--broker" {
            broker = value_for(&args, &mut i, "--broker")?;
        } else if let Some(name) = arg.strip_prefix("--") {
            let name = name.to_string();
            let value = value_for(&args, &mut i, arg)?;
            flags.push((name, value));
        } else {
            positional.push(arg.clone());
        }
        i += 1;
    }

    if positional.is_empty() {
        print!("{}", USAGE);
        return Ok(());
    }

    let command = positional[0].as_str();
    let rest = &positional[1..];

    match command {
        "create-topic" => {
            let name = require(rest, 0, "create-topic needs a topic name")?;
            let partitions = flag_u64(&flags, "partitions")?.unwrap_or(1) as u32;

            let mut producer = Producer::connect(&broker).await?;
            producer.create_topic(&name, partitions).await?;
            println!("created topic '{}' with {} partition(s)", name, partitions);
        }

        "list-topics" => {
            let mut producer = Producer::connect(&broker).await?;
            let topics = producer.list_topics().await?;
            if topics.is_empty() {
                println!("no topics");
                return Ok(());
            }
            println!(
                "{:<24} {:>10} {:>12} {:>12}",
                "TOPIC", "PARTITIONS", "MESSAGES", "BYTES"
            );
            for topic in topics {
                let messages: u64 = topic
                    .partitions
                    .iter()
                    .map(|p| p.next_offset - p.start_offset)
                    .sum();
                let bytes: u64 = topic.partitions.iter().map(|p| p.size_bytes).sum();
                println!(
                    "{:<24} {:>10} {:>12} {:>12}",
                    topic.name,
                    topic.partitions.len(),
                    messages,
                    bytes
                );
            }
        }

        "describe" => {
            let name = require(rest, 0, "describe needs a topic name")?;
            let mut producer = Producer::connect(&broker).await?;
            let topic = producer.describe_topic(&name).await?;

            println!("topic: {}", topic.name);
            println!(
                "{:<12} {:>14} {:>14} {:>12} {:>10}",
                "PARTITION", "START OFFSET", "NEXT OFFSET", "BYTES", "SEGMENTS"
            );
            for p in topic.partitions {
                println!(
                    "{:<12} {:>14} {:>14} {:>12} {:>10}",
                    p.partition, p.start_offset, p.next_offset, p.size_bytes, p.segments
                );
            }
        }

        "produce" => {
            let topic = require(rest, 0, "produce needs a topic name")?;
            let message = require(rest, 1, "produce needs a message")?;

            let mut producer = Producer::connect(&broker).await?;
            let offset = match flag_u64(&flags, "partition")? {
                Some(partition) => producer.send_to(&topic, partition as u32, &message).await?,
                None => producer.send(&topic, &message).await?,
            };
            println!("wrote to offset {}", offset);
        }

        "consume" => {
            let topic = require(rest, 0, "consume needs a topic name")?;
            let partition = flag_u64(&flags, "partition")?.unwrap_or(0) as u32;
            let from = flag_u64(&flags, "from")?.unwrap_or(0);
            let max = flag_u64(&flags, "max")?.unwrap_or(100) as usize;

            let mut consumer = Consumer::connect(&broker).await?;
            let records = consumer
                .poll_partition(&topic, partition, from, max)
                .await?;

            if records.is_empty() {
                println!("no messages at or after offset {}", from);
                return Ok(());
            }
            for record in records {
                println!("{}:{} {}", record.partition, record.offset, record.value);
            }
        }

        "groups" => {
            let mut consumer = Consumer::connect(&broker).await?;
            let groups = consumer.list_groups().await?;
            if groups.is_empty() {
                println!("no consumer groups");
                return Ok(());
            }
            println!("{:<28} {:>12} {:>10}", "GROUP", "GENERATION", "MEMBERS");
            for group in groups {
                println!(
                    "{:<28} {:>12} {:>10}",
                    group.group, group.generation, group.members
                );
            }
        }

        "describe-group" => {
            let name = require(rest, 0, "describe-group needs a group name")?;
            let mut consumer = Consumer::connect(&broker).await?;
            let (description, committed) = consumer.describe_group(&name).await?;

            println!("group: {}", description.group);
            println!("generation: {}", description.generation);

            if description.members.is_empty() {
                println!("\nno live members");
            } else {
                println!("\n{:<24} {:<20} PARTITIONS", "MEMBER", "TOPICS");
                for member in description.members {
                    let partitions: Vec<String> = member
                        .partitions
                        .iter()
                        .map(|tp| format!("{}:{}", tp.topic, tp.partition))
                        .collect();
                    println!(
                        "{:<24} {:<20} {}",
                        member.member_id,
                        member.topics.join(","),
                        if partitions.is_empty() {
                            "none".to_string()
                        } else {
                            partitions.join(" ")
                        }
                    );
                }
            }

            if !committed.is_empty() {
                println!("\n{:<24} {:>12} {:>16}", "TOPIC", "PARTITION", "COMMITTED");
                for row in committed {
                    println!("{:<24} {:>12} {:>16}", row.topic, row.partition, row.offset);
                }
            }
        }

        other => {
            eprintln!("unknown command '{}'\n", other);
            print!("{}", USAGE);
            process::exit(2);
        }
    }

    Ok(())
}

fn value_for(args: &[String], i: &mut usize, flag: &str) -> Result<String> {
    *i += 1;
    args.get(*i)
        .cloned()
        .ok_or_else(|| kafka_rust::Error::Protocol(format!("{} needs a value", flag)))
}

fn require(rest: &[String], index: usize, message: &str) -> Result<String> {
    rest.get(index)
        .cloned()
        .ok_or_else(|| kafka_rust::Error::Protocol(message.to_string()))
}

fn flag_u64(flags: &[(String, String)], name: &str) -> Result<Option<u64>> {
    match flags.iter().find(|(k, _)| k == name) {
        None => Ok(None),
        Some((_, value)) => value.parse::<u64>().map(Some).map_err(|_| {
            kafka_rust::Error::Protocol(format!("--{} expects a number, got '{}'", name, value))
        }),
    }
}
