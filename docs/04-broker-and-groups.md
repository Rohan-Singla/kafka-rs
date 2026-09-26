# 4. The broker and consumer groups

Files: `src/broker/`.

## The topic registry

```rust
pub struct Broker {
    config: BrokerConfig,
    topics: DashMap<String, Vec<Arc<RwLock<Partition>>>>,
    offsets: Arc<OffsetStore>,
    coordinator: Coordinator,
    create_lock: tokio::sync::Mutex<()>,
}
```

`DashMap` is a concurrent hash map: it shards internally so two threads touching
different topics never contend. A plain `HashMap` behind one lock would make the
registry itself a bottleneck even when the partitions being written are
unrelated.

Each partition then has its own `RwLock`, so locking is per partition rather
than global.

### Rebuilding it on startup

`load_topics_from_disk` walks the data directory and rebuilds the registry from
whatever folders are there. Without this step the registry starts empty and a
restarted broker says "unknown topic" for data it is literally sitting on.

It skips two things:

- Directories whose name is not a valid topic name.
- Topics with non-contiguous partition folders. Partition ids index into a
  `Vec`, so a missing `partition-1` would silently shift `partition-2` into slot
  1 and hand readers the wrong log. Refusing to load is the safe answer.

### Creating a topic twice

`create_topic` with the same partition count is a no-op, so clients can call it
on startup without checking first. With a **different** count it is an error.

Returning success there would be a lie with consequences: the caller believes it
has 8 partitions, produces to partition 5, and gets "no partition 5" from a
topic it just successfully created. Refusing puts the error where the mistake
is.

The whole check-and-create runs under a mutex. Topic creation is rare, so a
single lock costs nothing, and it removes a race where two connections both see
"topic does not exist" and both open the same segment files. The second insert
would win the registry while a producer kept appending through the first,
orphaned `Partition` with its own idea of the next offset.

### Topic names are validated

```rust
pub fn validate_topic_name(name: &str) -> Result<()>
```

Allows letters, digits, `.`, `_`, `-` and nothing else. This is a security
check, not tidiness: the name becomes a directory name, so `../../etc/passwd`
would otherwise write outside the data directory.

## Durable consumer offsets

`src/broker/offsets.rs`. Every commit appends one JSON line:

```
{"g":"order-processors","t":"orders","p":0,"o":6}
{"g":"order-processors","t":"orders","p":1,"o":3}
```

On startup the file is replayed with **last write wins**, then rewritten from
the resulting state. That rewrite is compaction: without it the file grows
forever, since a consumer committing once a second writes 86,400 lines a day
while only the final one matters.

Compaction on startup alone is not enough, because a broker that stays up for
weeks never gets one. So `commit` also compacts in place once the journal has
taken 10,000 appends *and* holds at least twice as many lines as there are
distinct group/topic/partition keys. The second condition is what stops a
workload with 50,000 real keys from recompacting on every commit: the trigger
has to be redundancy, not size alone.

The rewrite goes to a temp file and then `rename`, which is atomic on POSIX. A
crash mid compaction leaves either the old complete file or the new complete
file, never a half written one.

A torn final line (a crash mid commit) is logged and skipped rather than treated
as fatal. The cost of skipping is one consumer replaying a few messages. The
cost of bailing out would be every group losing its position.

This is the same idea as Kafka's compacted `__consumer_offsets` topic, done with
a flat file.

## Consumer groups

`src/broker/groups.rs`.

### State

```rust
struct Group {
    generation: u64,
    members: HashMap<String, Member>,
    assignments: HashMap<String, Vec<TopicPartition>>,
}
```

The **generation** is a counter bumped on every membership change. It is how a
consumer discovers it has been rebalanced: it heartbeats, gets back a generation
it does not recognise, and adopts the new assignment.

### The assignment algorithm

Round robin, dealt per topic:

```
for each topic anyone subscribed to:
    eligible = members subscribed to THIS topic, sorted by id
    for partition in 0..partition_count:
        owner = eligible[partition % eligible.len()]
        give partition to owner
```

Two properties matter:

- **Per topic.** A member only ever receives partitions of topics it actually
  subscribed to. Dealing globally would hand a member a topic it never asked
  for.
- **Sorted.** Members sorted by id and partitions in numeric order makes the
  result deterministic, so the same membership always produces the same
  assignment.

Worked example, 6 partitions and 3 members:

```
partition 0 -> member A     partition 3 -> member A
partition 1 -> member B     partition 4 -> member B
partition 2 -> member C     partition 5 -> member C
```

With 2 partitions and 4 members, two members get one partition each and two get
nothing. That is not a bug: a partition can only be owned by one member of a
group, so partition count is the parallelism ceiling.

### Eviction

A member that stops heartbeating is dropped after the session timeout (30s by
default) and its partitions are redistributed.

From the evicted client's side this is survivable. `Consumer::poll` heartbeats
first, so an eviction shows up as an `unknown_member` error on the next poll,
and the consumer rejoins the group under a fresh member id instead of returning
that error to the caller forever. It resumes from the committed offset, which is
the honest answer: anything polled but not committed is delivered again.

Eviction is **lazy**: it happens at the start of each `join` and `heartbeat`
rather than on a timer. There is no background thread, and nothing is missed,
because the surviving members are heartbeating and each of those calls triggers
the sweep. A group with no live members has nobody to notice, and also nobody
who cares.

`leave` is the polite path. It removes the member immediately and rebalances, so
the group recovers straight away instead of waiting out the timeout.

### Delivery is at least once

Worth stating plainly, because it is a property of the design rather than an
oversight. A rebalance can move a partition to another member while the previous
owner has polled records it has not committed. Those records are handed to the
new owner as well, so the application sees them twice.

Nothing here silently commits on your behalf to paper over that. A commit means
"I have finished with everything before this offset", and only the application
knows when that is true. Committing automatically on revocation would turn
redelivery into *silent message loss* whenever a consumer was interrupted
mid-work, which is the strictly worse failure.

So the contract is: handlers should be idempotent, and `commit()` should be
called at whatever boundary makes them so. Exactly-once would need the commit
and the side effect to land atomically, which is a transaction protocol this
project does not have.

## The concurrency model

Three decisions, in order of importance.

### 1. Reads share, writes exclude

```rust
Arc<RwLock<Partition>>
```

Many readers or one writer. `Partition::read_from` takes `&self`, so consumers
genuinely read in parallel with each other while a producer appends.

This only works because of positional I/O in the storage layer. See
[06-rust-notes.md](06-rust-notes.md) for why that is the load bearing detail.

### 2. Disk I/O runs on the blocking pool

```rust
spawn_blocking(move || {
    let mut guard = handle.write().unwrap_or_else(|e| e.into_inner());
    guard.append(&value)
}).await
```

Tokio runs many connections on a few worker threads. A blocking file write on a
worker thread parks *every other connection that worker was driving*, not just
this one. `spawn_blocking` moves it to a separate pool built for exactly this.

`unwrap_or_else(|e| e.into_inner())` recovers from lock poisoning. If a thread
panicked while holding the lock, Rust marks it poisoned and normally every later
`lock()` fails forever. Here the partition's state is only updated after a
successful write, so recovering is safe and beats bricking the partition.

### 3. Fetches are clamped twice

```rust
let max_count = max_count.clamp(1, MAX_FETCH_COUNT);
guard.read_from(offset, max_count, MAX_FETCH_BYTES)
```

A count cap alone stops `usize::MAX` records being requested, but 10,000 records
of 8MB each still comes to 80GB. The byte budget is the cap that bounds memory.

It does **not** bound the frame, and that catch is worth understanding. The
budget counts bytes on disk, but the response carries payloads as JSON strings,
and JSON escaping expands a control character sixfold: one stored byte becomes
the six characters `\u0001`. So 4MB of the wrong bytes serializes to 24MB and
overruns the 16MB frame.

The frame is bounded separately, in `fetch_response`, which measures each
message's *escaped* length and stops before the limit. Two different budgets
because there are two different resources: memory on the way out of the disk,
and the frame on the way out of the socket.

### Anything accepted must be readable back

There is a trap in having two limits. A produce request and a fetch response
carry different fixed overhead, so a payload can fit inside an incoming frame
and then fail to fit in the response that returns it. Stored successfully,
impossible to read: the consumer hits that offset and stops there forever, and
every later message on the partition is stranded behind it.

So produce enforces the *fetch* budget, not its own. If a message could not be
delivered back, it is refused at write time, where the error is actionable.

`fetch_response` still guards the case, since a record could predate the check
or be written through the library directly rather than over the wire. It names
the offset and the offset to resume from.

Be aware of what that recovery costs. The resume offset is prose inside an error
string, and `Consumer::poll` propagates a fetch failure with `?`, so one
poisoned partition aborts the whole poll and starves the member's other
partitions too. Stepping over such a record means reading the offset out of the
message and driving `poll_partition` directly rather than using `poll`.

`commit_offset` also goes through `spawn_blocking`, for the same reason as
`produce`: with `--fsync` on, it does a real disk sync.

## fsync, and the 366x

Off by default. The measured difference:

| Mode | msgs/sec | p99 |
|---|---|---|
| Default | 67,738 | 112µs |
| `--fsync` | 185 | 34,879µs |

Understanding why needs two separate ideas that get conflated:

- **A normal write reaches the kernel's page cache.** If the *broker process*
  dies, the data is fine. The OS still has it and will write it out. `kill -9`
  is survivable.
- **It has not reached the physical disk yet.** If the *machine* loses power,
  it is gone. Only `fsync` forces it to the platter.

So the default is safe against a crash and unsafe against a power cut, and
closing that gap costs three orders of magnitude.

Kafka makes the same choice and defaults the same way, because it has
replication: a write living in the page cache of three machines is safe against
any one of them losing power. This project is single node, so it does not have
that fallback. That is the honest caveat, and it is why "no replication" is
listed as the biggest gap in the README.

Next: [05-code-tour.md](05-code-tour.md).
