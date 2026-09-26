# 1. The mental model

Before any code, the five words you need.

## A message broker is a shared notebook

Imagine a notebook where writers only ever add lines to the end, never edit or
delete. Readers each keep a bookmark saying which line they got to.

That is a message broker. The notebook is the log. The line number is the
offset. The bookmark is the committed offset.

Everything else exists to make that idea fast and crash proof.

## The five terms

### Topic

A named category. `orders`, `payments`, `signups`. On disk it is literally just
a folder:

```
data/orders/
```

Nothing more. A topic is a name and a folder.

### Partition

A topic is split into N partitions so several writers and readers can work at
once. Each partition is a subfolder and its own independent log:

```
data/orders/partition-0/
data/orders/partition-1/
data/orders/partition-2/
```

This is the single most important structural idea in the project. Partitions
are why the thing can go fast: three partitions means three separate files that
three producers can append to simultaneously without waiting on each other.

**The cost:** ordering is only guaranteed *inside* one partition. If you send
`created`, `paid`, `shipped` for the same order and they land on three different
partitions, a consumer may read them in any order. That is why the producer has
two methods:

```rust
producer.send("orders", "order #1").await?;               // round robin, fast, unordered across partitions
producer.send_to("orders", 0, "order #42 paid").await?;   // pinned to partition 0, keeps order
```

### Offset

The position of a message inside one partition. Starts at 0, goes up by one per
message, never reused.

Offsets are **per partition**, not global. Partition 0 and partition 1 both have
an offset 0, holding different messages. This trips people up constantly.

```
partition-0:  offset 0, 1, 2, 3, ...
partition-1:  offset 0, 1, 2, ...        <- different messages
```

### Segment

A partition's log is not one giant file. It is split into segments, each capped
at 64MB. One segment is a pair of files:

```
data/orders/partition-0/
├── 00000000000000000000.log    <- the messages
└── 00000000000000000000.idx    <- offset -> byte position lookup table
```

The filename is the first offset in that segment, zero padded so a directory
listing sorts correctly. Only the newest segment is written to. Once it passes
64MB the broker seals it and starts a new one:

```
00000000000000000000.log     <- sealed, offsets 0 to 481,231
00000000000000481232.log     <- active, being appended to
```

**Why bother splitting?** So old data can eventually be deleted a chunk at a
time, and so no single file grows unbounded. Deleting old segments is not built
yet, but the layout is ready for it.

### Consumer group

Several consumers cooperating so each message is handled once by the group.

Give two consumers the same group name against a 4 partition topic and the
broker assigns 2 partitions to each. Neither reads the other's partitions, so no
message is processed twice.

```
group "order-processors"
├── consumer A  ->  partition 0, partition 2
└── consumer B  ->  partition 1, partition 3
```

Add a third consumer and the broker redistributes. Kill one and its partitions
get handed to the survivors. That redistribution is called a **rebalance**.

**The ceiling:** a partition is owned by exactly one consumer in a group at a
time. So a 4 partition topic can use at most 4 consumers per group. A 5th sits
idle. Partition count is your parallelism budget, chosen when you create the
topic.

## How the pieces move

Writing:

```
producer.send("orders", "hello")
   -> pick partition (round robin)
   -> broker appends to that partition's active segment
   -> broker returns the offset it landed at
```

Reading:

```
consumer.subscribe("my-group", ["orders"])
   -> broker assigns it some partitions
   -> for each, broker looks up the group's committed offset
consumer.poll(100)
   -> fetch from the current position
   -> returns records, position moves forward
consumer.commit()
   -> broker writes the position to disk
```

The commit is what makes a restart resume instead of replaying. It is stored in
`data/__offsets.json`, keyed by group, topic and partition.

## Two things that surprise people

**Reading does not delete.** Consuming a message leaves it on disk. Ten
different groups can read the same message independently. A brand new group
starts at offset 0 and replays everything ever written. This is why the thing is
useful as a durable event log rather than just a queue.

**The broker does not track consumers.** It stores a number per group per
partition. That is all. The consumer decides what to do with it. This is what
keeps the broker simple and fast, and it is a real design difference from
traditional message queues that track per message acknowledgement.

Next: [02-storage-engine.md](02-storage-engine.md), where the actual bytes live.
