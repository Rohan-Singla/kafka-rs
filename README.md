# kafka-rust

A message broker built from scratch in Rust, inspired by Apache Kafka. Not a wrapper around Kafka. Not a tutorial clone. The actual storage engine, binary protocol, and TCP server written from the ground up.

Built as a systems programming portfolio project to understand how message brokers actually work under the hood.

## Why

Real Kafka runs on the JVM. The garbage collector randomly pauses to clean up memory, which causes latency spikes when you are moving millions of messages per second. Rust has no garbage collector, so latency stays predictable under any load.

The other reason is concurrency. Multiple producers writing to the same partition simultaneously is a classic data race. In most languages this compiles fine and corrupts data silently at runtime. Rust makes the incorrect version impossible to compile.

## What It Does

Producers connect over TCP and write messages to named topics. The broker stores those messages on disk in an append only log. Consumers connect and read from any offset they choose. Multiple consumers in the same group split the work across partitions without reading the same message twice.

Messages stay on disk. A consumer that crashes and restarts picks up exactly where it left off. A new consumer can replay from offset zero and read every message ever written.

## How It Works

```
Producer  ──TCP──►  Broker  ──►  disk (.log files)
                      │
Consumer  ◄──TCP──   │  ◄──  disk (.log files)
```

The broker is the only process that touches disk. Producers and consumers are clients that talk to it over TCP using a simple binary protocol.

## Architecture

### Storage Layer

The core of the project. Every message ends up here.

A topic is a folder on disk. A partition is a subfolder inside the topic. Inside each partition there are segment files.

```
data/
└── orders/
    └── partition-0/
        ├── 00000000000000000000.log
        └── 00000000000000000000.idx
```

A segment is one log file paired with one index file. When a segment grows too large the broker closes it and starts a new one. Old segments stay on disk so consumers can still read them.

The log file stores the actual messages one after another in binary:

```
offset (8 bytes) | timestamp (8 bytes) | length (4 bytes) | message (N bytes)
```

The index file maps each offset to its byte position in the log file:

```
offset (8 bytes) | byte position (8 bytes)
```

Every index entry is exactly 16 bytes. To find offset 500, seek to byte 8000 in the index, read 8 bytes, and you have the exact position in the log file. No scanning. Constant time lookup regardless of how many messages exist.

### Network Layer

A Tokio async TCP server. Each incoming connection gets its own async task instead of its own thread. A thread in Java costs around 1MB of stack. A Tokio task costs a few kilobytes. Ten thousand simultaneous connections stay manageable.

The protocol is a simple binary framing:

```
total length (4 bytes) | api key (1 byte) | payload (JSON)
```

Api keys map to operations: produce, fetch, create topic, commit offset, fetch offset.

### Broker Core

The broker owns the topic registry, a concurrent hashmap that maps topic names to their partitions. Multiple producers can write to different partitions simultaneously without any locking on the registry itself.

Each partition has its own lock. Writers take an exclusive lock. Readers take a shared lock. Multiple consumers can read from the same partition at the same time.

### Client Library

The same crate exposes a producer and consumer client so other Rust projects can import it directly.

```rust
let producer = Producer::connect("127.0.0.1:9092").await?;
producer.send("orders", "order #101").await?;

let consumer = Consumer::connect("127.0.0.1:9092").await?;
let messages = consumer.poll("orders", 0).await?;
```

## Running It

Start the broker:

```bash
cargo run --bin broker
```

Run the producer example:

```bash
cargo run --example producer
```

Run the consumer example:

```bash
cargo run --example consumer
```

## Tech Stack

| Crate | Purpose |
|---|---|
| tokio | Async runtime for TCP connections |
| serde + serde_json | Protocol payload encoding |
| dashmap | Concurrent topic registry with no manual locking |
| bytes | Byte buffer handling for the network layer |
| crc32fast | Message checksums to detect disk corruption |
| tracing | Structured logging |

## Project Status

Storage layer is in progress. Network layer and broker core come next.

```
storage/segment.rs    in progress
storage/partition.rs  todo
network/              todo
broker/               todo
producer client       todo
consumer client       todo
consumer groups       todo
```

## Benchmarks

Coming once the broker is functional. Target is sustained throughput with sub millisecond p99 latency compared against Redis pub/sub.
