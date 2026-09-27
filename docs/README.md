# mini-kafka docs

Notes for understanding this project, written for the person who has to come
back to it in six months.

The code itself has no comments, so this folder is where the reasoning lives.
If you ever wonder "why is it done this way", the answer should be here.

## Read in this order

| | File | What it covers |
|---|---|---|
| 1 | [01-mental-model.md](01-mental-model.md) | Topics, partitions, offsets, segments, groups. Start here. |
| 2 | [02-storage-engine.md](02-storage-engine.md) | The bytes on disk, the index trick, crash recovery. The heart of the project. |
| 3 | [03-network-protocol.md](03-network-protocol.md) | Framing, the wire format, every request type, driving it by hand. |
| 4 | [04-broker-and-groups.md](04-broker-and-groups.md) | Topic registry, durable offsets, group assignment, the concurrency model. |
| 5 | [05-code-tour.md](05-code-tour.md) | File by file, what to read and in what order. |
| 6 | [06-rust-notes.md](06-rust-notes.md) | The Rust concepts this project leans on, explained plainly. |

## The 30 second version

A producer sends a message over TCP. The broker appends it to a file and
returns the position it landed at, called the offset. A consumer asks for
"everything from offset N" and gets it back. Consumers remember their own
position, so they can stop and resume, or replay from the beginning.

That is the whole idea. Everything else is making it fast, making it survive a
crash, and letting several consumers share the work.

## Running it

```bash
cargo run --release --bin broker          # terminal 1
cargo run --release --example producer    # terminal 2
cargo run --release --example consumer
```

```bash
cargo test                                # 75 tests
cargo clippy --all-targets                # clean
cargo fmt --check                         # clean
```

