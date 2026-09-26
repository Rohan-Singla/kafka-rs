# mini-kafka

A message broker built from scratch in Rust, inspired by Apache Kafka. Not a wrapper around Kafka. Not a tutorial clone. The storage engine, crash recovery, wire protocol, TCP server, consumer group coordinator and client library are all written from the ground up.

Built as a systems programming portfolio project to understand how message brokers actually work under the hood.

```
115,000 messages/sec produced, p99 430 microseconds
830,000 messages/sec consumed
zero dependencies beyond tokio, serde, dashmap, bytes and crc32fast
```

## Quick start

```bash
# terminal 1
cargo run --release --bin broker

# terminal 2
cargo run --release --example producer
cargo run --release --example consumer
```

Then poke at it with the admin CLI:

```bash
cargo run --release --bin kafka-cli -- list-topics
cargo run --release --bin kafka-cli -- describe orders
cargo run --release --bin kafka-cli -- describe-group order-processors
```

## Why

Real Kafka runs on the JVM. The garbage collector pauses to reclaim memory, and those pauses land in the tail of your latency distribution exactly when you are moving the most data. Rust has no garbage collector, so the p99 is a property of the work being done rather than of when the runtime decides to collect.

The other reason is concurrency. Multiple producers appending to one partition is a textbook data race, and multiple consumers reading it while that happens is another. In most languages both compile fine and corrupt data at runtime under load. Here the type system does not let the broken version build, which is why the concurrency design below could be aggressive about shared reads instead of defensively locking everything.

## What it does

Producers connect over TCP and write messages to named topics. The broker appends them to a log on disk. Consumers read from any offset they choose, either directly or as part of a group that splits the partitions between its members.

Messages live on disk, not in memory. A consumer that crashes resumes from its last committed offset. A broker that is killed with `SIGKILL` mid write comes back up, repairs its log, and serves the data that survived. A new consumer group can replay every message ever written from offset zero.

## Architecture

```
Producer  ──TCP──►  Broker  ──►  segment files (.log + .idx)
                      │
Consumer  ◄──TCP──────┘  ◄──  segment files
```

The broker is the only process that touches disk. Everything else is a client.

### Storage layer

A topic is a directory. A partition is a subdirectory. A partition is an ordered run of segments, and a segment is one `.log` file paired with one `.idx` file.

```
data/
└── orders/
    └── partition-0/
        ├── 00000000000000000000.log
        └── 00000000000000000000.idx
```

Filenames are the zero padded base offset of the segment, so a directory listing sorts in offset order. Only the last segment is written to. When it passes 64MB the broker seals it and starts a new one; sealed segments stay on disk so consumers can still replay them.

**Record format in the `.log`:**

```
offset (8) | timestamp (8) | crc32 (4) | length (4) | payload (N)
```

**Entry format in the `.idx`:**

```
offset (8) | byte position in the log (8)
```

Every index entry is exactly 16 bytes, so finding offset N means seeking to byte `N * 16`. No scanning, no search, constant time regardless of how many messages exist. A point read is two positional reads: one 8 byte read of the index to get the position, one read of the record at that position.

Sequential reads skip the index entirely after the first lookup. Records are laid out back to back, so once you know where one record starts you know where the next one does.

### Crash recovery

The log is the source of truth and the index is a derived accelerator. That ordering is what makes recovery tractable: the index can always be rebuilt from the log, never the other way around.

On open, a segment replays its log from the start, validating each record's checksum and checking that offsets are contiguous. It stops at the first record that does not check out, truncates the log to that boundary, and rewrites the index from what survived.

That single pass handles every way a crash can tear the tail:

- **A half written header or payload.** The length field runs past the end of the file, so the record is dropped.
- **A record that landed but whose index entry did not.** Appends write the log before the index, so the index is only ever short, never pointing at bytes that were never written. The rebuild fills it back in.
- **Silent bit rot.** The checksum covers the offset, timestamp and length alongside the payload, so corruption in the framing metadata is caught too, not just corruption in the data.

The checksum is also what makes truncation safe rather than reckless. Without it, a garbled length field is indistinguishable from a real one, and recovery would either trust it and read nonsense or refuse to open the segment at all.

Consumer offsets get the same treatment. They are appended to a journal file, replayed last write wins on startup, and compacted back down so the file does not grow forever. A partly written final line is skipped, which costs one consumer a few replayed messages instead of losing every group's position.

### Concurrency

Every file operation is a positional read or write, naming the byte it acts on instead of moving a shared cursor. That is what lets `Segment::read` take `&self` rather than `&mut self`, which in turn lets the broker hold each partition behind a `RwLock` where reads genuinely share.

The practical result: any number of consumers can read from a partition at the same time as a producer appends to it, and they do not queue behind each other. Writers still take the exclusive lock, because appending advances the segment's end position.

Disk I/O runs on Tokio's blocking pool rather than inline on a worker thread. Doing a synchronous file write on an async worker parks every other connection that worker was driving, which is the difference between one slow request and a stalled broker.

### Network layer

A Tokio TCP server. Every connection gets its own task instead of its own thread. A thread costs around 1MB of stack; a task costs a few kilobytes, so idle connections are close to free.

Framing is a length prefix:

```
length (4 bytes, big endian) | payload (JSON)
```

TCP is a byte stream with no message boundaries, so the length prefix is what tells the reader where one request ends and the next begins. The prefix is capped at 16MB: without a ceiling, four bytes claiming `u32::MAX` would ask the broker to allocate 4GB before a single byte of body arrived.

The payload is JSON tagged with a `type` field, so a frame is self describing and the protocol can be driven by hand for debugging. This trades throughput for legibility, which is the right trade at this scale and the first thing to replace if it were not.

Operations: `CreateTopic`, `Produce`, `Fetch`, `CommitOffset`, `FetchOffset`, `ListTopics`, `DescribeTopic`, `JoinGroup`, `Heartbeat`, `LeaveGroup`, `ListGroups`, `DescribeGroup`.

### Broker core

A `DashMap` maps topic names to their partitions, so producers writing to different topics never contend on the registry itself. Each partition carries its own lock.

On startup the broker scans its data directory and rebuilds the registry from what is there. Topic names are validated before they ever become directory names, since a name like `../../etc/passwd` is otherwise a path traversal straight out of the data directory.

### Consumer groups

A group coordinator tracks membership and hands each member a slice of the partitions. Assignment is round robin, dealt per topic so a member only ever receives partitions of topics it actually subscribed to, and computed from sorted member ids so it is deterministic.

Membership changes bump a generation counter. Consumers heartbeat on every poll, and a heartbeat that comes back with an unfamiliar generation is how a consumer learns it has been rebalanced. A member that stops heartbeating is evicted after the session timeout and its partitions are redistributed.

When a consumer picks up a new partition it seeds its position from the group's committed offset, so a rebalance does not replay the partition from zero.

## Client library

The same crate exposes the producer and consumer, so other Rust projects can import it directly.

```rust
use kafka_rust::client::{Producer, Consumer};

let mut producer = Producer::connect("127.0.0.1:9092").await?;
producer.create_topic("orders", 3).await?;

producer.send("orders", "order #101").await?;          // round robin
producer.send_to("orders", 0, "order #101 paid").await?; // pinned, keeps order
```

```rust
let mut consumer = Consumer::connect("127.0.0.1:9092").await?;
consumer.subscribe("order-processors", &["orders"]).await?;

for record in consumer.poll(100).await? {
    println!("{}:{} {}", record.partition, record.offset, record.value);
}
consumer.commit().await?;
```

`send` rotates through partitions so the logs fill evenly, which is what makes a consumer group able to parallelise at all. Ordering is only ever guaranteed within a single partition, so anything order sensitive uses `send_to` and pins itself to one.

`poll` returns one partition's batch at a time and rotates between the assigned partitions, so a partition with a long backlog cannot starve the others.

To replay a log with no group involved:

```rust
let all = consumer.poll_partition("orders", 0, 0, 1000).await?;
```

## Admin CLI

```bash
kafka-cli create-topic orders --partitions 3
kafka-cli list-topics
kafka-cli describe orders
kafka-cli produce orders "order #101" [--partition 0]
kafka-cli consume orders --partition 0 --from 0 --max 100
kafka-cli groups
kafka-cli describe-group order-processors
```

All commands take `--broker <addr>`, defaulting to `127.0.0.1:9092`.

## Benchmarks

Measured over loopback TCP against a release build. Apple M5, 10 cores, macOS 26.6. 128 byte payloads, one partition per producer, `bench` binary in this repo.

Every message is a full request and response round trip. There is no batching, so these are per message numbers, not amortised ones.

**Produce**

| Producers | msgs/sec | MB/sec | p50 | p95 | p99 |
|---|---|---|---|---|---|
| 1  | 30,722  | 3.75  | 29µs  | 42µs  | 61µs  |
| 2  | 55,652  | 6.79  | 34µs  | 47µs  | 62µs  |
| 4  | 67,738  | 8.27  | 54µs  | 87µs  | 112µs |
| 8  | 91,173  | 11.13 | 81µs  | 133µs | 187µs |
| 16 | 115,233 | 14.07 | 122µs | 234µs | 430µs |

Throughput scales close to linearly to 8 producers and then flattens as the 10 core machine saturates. The p99 stays under half a millisecond throughout.

**Consume**

830,710 msgs/sec, 101 MB/sec, reading back 200,000 messages in batches of 500. Reads are an order of magnitude faster than writes because a fetch amortises one round trip across the whole batch and the records come back off sequential pages.

**The cost of durability**

| Mode | msgs/sec | p99 |
|---|---|---|
| Default (write reaches the OS) | 67,738 | 112µs |
| `--fsync` (write reaches the platter) | 185 | 34,879µs |

A 366x difference, and the single most important number here. By default a write is safe against the broker process dying, because it has already been handed to the kernel. It is not safe against the machine losing power. `--fsync` closes that gap and costs three orders of magnitude to do it. Kafka makes the same trade and defaults the same way, leaning on replication rather than fsync for durability.

Reproduce:

```bash
cargo run --release --bin broker
cargo run --release --bin bench -- --messages 200000 --producers 16
```

## Tests

```bash
cargo test
```

67 tests: 52 unit, 15 integration.

The integration tests boot a real broker on an ephemeral port and drive it with the real client over a real socket. The ones worth reading:

- `messages_and_committed_offsets_survive_a_restart` stops the broker, restarts it against the same directory, and asserts the consumer resumes at its committed offset rather than replaying.
- `a_consumer_group_splits_partitions_without_double_reading` puts two consumers in a group against four partitions and asserts all 40 messages are read exactly once.
- `concurrent_producers_do_not_lose_or_duplicate_offsets` has eight producers hammer one partition and asserts every offset from 0 to 399 was handed out exactly once.
- `an_oversized_frame_is_refused` sends a `u32::MAX` length prefix and asserts the broker refuses it and keeps serving other clients.

On the storage side, `recovery_truncates_a_torn_tail` simulates a crash partway through an append, `recovery_stops_at_a_flipped_bit` flips a byte in a payload, and `index_is_rebuilt_from_the_log` deletes the index outright. All three assert the surviving data is still readable.

## Project layout

```
src/
├── error.rs                 manual error type, no thiserror
├── storage/
│   ├── record.rs            on-disk record format and checksums
│   ├── fileio.rs            positional reads and writes, unix and windows
│   ├── segment.rs           one .log + .idx pair, recovery, reads
│   └── partition.rs         an ordered run of segments, rolling
├── broker/
│   ├── mod.rs               topic registry, produce and fetch
│   ├── offsets.rs           durable consumer offset journal
│   └── groups.rs            group membership and partition assignment
├── network/
│   ├── protocol.rs          request and response types
│   ├── codec.rs             length prefixed framing
│   └── mod.rs               TCP server and dispatch
├── client/
│   ├── producer.rs
│   └── consumer.rs
└── bin/
    ├── broker.rs            the daemon
    ├── kafka-cli.rs         admin CLI
    └── bench.rs             load generator
```

## Design notes and limitations

Things that are deliberate, and things that are simply not built yet.

**Single node.** No replication, no leader election, no ISR. This is the single biggest gap between this and real Kafka, and it is what the fsync default above is really about: Kafka can afford a loose fsync policy because it has replicas, and this cannot.

**JSON on the wire.** Readable and debuggable, and measurably slower than a binary encoding. The framing layer is separate from the payload encoding, so swapping it is contained. Message payloads are UTF-8 text at the protocol level even though the storage engine underneath is byte oriented.

**No batching.** Every message is its own round trip. Batched produce is the single largest throughput win still on the table and would likely be worth several times the current numbers.

**No retention policy.** Segments accumulate forever. Deleting sealed segments past an age or size bound is the natural next piece, and the segment layout was built with it in mind.

**Offsets are dense in the index.** One 16 byte index entry per message. Real Kafka indexes sparsely and scans the gap, trading a little read time for a much smaller index.

**Group assignment is round robin only.** No sticky assignment, so a rebalance can move a partition that did not need to move.

### Next

- Batch produce and fetch
- Key based partitioning, so related messages land on the same partition by hash
- Retention by age and size
- Sparse indexing
