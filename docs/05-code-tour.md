# 5. Code tour

Where everything lives and the order to read it in.

## Layout

```
src/
├── lib.rs                  7    module declarations, nothing else
├── error.rs               92    the one error type
├── storage/                      the disk engine
│   ├── record.rs          94    byte layout and checksums
│   ├── fileio.rs          58    positional read and write
│   ├── segment.rs        413    one .log + .idx pair, recovery
│   └── partition.rs      369    an ordered run of segments
├── broker/                       the coordination layer
│   ├── mod.rs            575    topic registry, produce, fetch
│   ├── offsets.rs        246    durable consumer offsets
│   └── groups.rs         481    membership and assignment
├── network/                      the wire
│   ├── protocol.rs       144    Request and Response types
│   ├── codec.rs          105    length prefixed framing
│   └── mod.rs            209    TCP server and dispatch
├── client/                       the library other code imports
│   ├── mod.rs             55    one connection, request and response
│   ├── producer.rs        82    send, send_to
│   └── consumer.rs       280    subscribe, poll, commit
└── bin/
    ├── broker.rs         130    the daemon
    ├── kafka-cli.rs      250    admin CLI
    └── bench.rs          260    load generator

tests/integration.rs      497    real broker, real socket
```

A lot of those numbers are tests. Every module keeps its unit tests in a
`#[cfg(test)] mod tests` block at the bottom, which is the normal Rust
convention: tests sit next to the thing they test and can reach private items.

## Reading order

### Start here, it is the whole idea in 90 lines

**`src/storage/record.rs`**

`encode` and `decode_header`. Once you see that a message is just
`offset | timestamp | crc | length | payload`, the rest of the storage layer
is mechanics.

### Then the file that matters most

**`src/storage/segment.rs`**

Read in this order:

1. `append`: write the log, then the index. 20 lines.
2. `read`: index lookup, then one positional read. 15 lines.
3. `recover`: the crash handling. This is the interesting one.

If you only re-read one function in this project, make it `recover`.

### Then how segments become a log

**`src/storage/partition.rs`**

`append` rolls to a new segment past the size cap. `read_from` walks forward
across segment boundaries, bounded by both a record count and a byte budget.

### Then the wire

**`src/network/codec.rs`** first, it is only 105 lines and explains why a length
prefix exists at all. Then **`protocol.rs`** for the request list, then
**`mod.rs`** for the server loop and `dispatch`.

### Then the coordination

**`src/broker/mod.rs`**, specifically `produce`, `fetch` and
`load_topics_from_disk`. Then **`offsets.rs`** (small, self contained). Leave
**`groups.rs`** for last, it is the most involved file and the least essential
to understanding the core.

### Then the client

**`src/client/consumer.rs`**, the `poll` method. It shows the group protocol
from the other side: heartbeat, notice the generation changed, adopt the new
assignment, read.

## Where to look when

| Question | File | Function |
|---|---|---|
| How is a message stored? | `storage/record.rs` | `encode` |
| What happens after a crash? | `storage/segment.rs` | `recover` |
| How is offset N found? | `storage/segment.rs` | `read` |
| When does a new segment start? | `storage/partition.rs` | `append` |
| How are messages framed on TCP? | `network/codec.rs` | `read_frame` |
| What requests exist? | `network/protocol.rs` | `enum Request` |
| How is a request handled? | `network/mod.rs` | `dispatch` |
| How do topics survive restart? | `broker/mod.rs` | `load_topics_from_disk` |
| How are offsets persisted? | `broker/offsets.rs` | `commit`, `replay` |
| How are partitions divided? | `broker/groups.rs` | `rebalance_with_known_counts` |
| How does a consumer rebalance? | `client/consumer.rs` | `poll` |

## The tests are documentation

Test names are written as sentences, so `cargo test` reads like a spec:

```
reopening_resumes_at_the_next_offset
recovery_truncates_a_torn_tail
recovery_stops_at_a_flipped_bit
index_is_rebuilt_from_the_log
a_hole_in_a_sealed_segment_does_not_hide_later_records
partitions_split_evenly_and_do_not_overlap
a_silent_member_is_evicted_and_its_work_reassigned
messages_and_committed_offsets_survive_a_restart
concurrent_producers_do_not_lose_or_duplicate_offsets
an_oversized_frame_is_refused
a_huge_fetch_is_capped_instead_of_dropping_the_connection
```

List them all:

```bash
cargo test -- --list
```

If you want to know whether a behaviour is intentional, look for a test with
its name. If one exists, it was deliberate.

## Making a change safely

1. `cargo test` before you start, so you know it was green.
2. Change one layer at a time. The layers only depend downward: storage knows
   nothing about the broker, the broker knows nothing about the network.
3. `cargo test` again. If a storage test broke, the bug is in storage, not
   three layers up.
4. `cargo clippy --all-targets` and `cargo fmt`.
5. For anything touching the disk format or recovery, run the real thing:
   start the broker, produce, `kill -9`, restart, check the data is there.

Next: [06-rust-notes.md](06-rust-notes.md).
