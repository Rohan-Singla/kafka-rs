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
cargo test                                # 61 tests
cargo clippy --all-targets                # clean
cargo fmt --check                         # clean
```

## The four things worth being able to explain

If someone asks you about this project in an interview, these are the answers
that show you understand it rather than just assembled it.

1. **Why the index makes lookups constant time.** Every index entry is exactly
   16 bytes, so finding offset N is arithmetic, not searching. See
   [02](02-storage-engine.md).

2. **Why a crash does not corrupt the log.** The log is the source of truth and
   the index is rebuilt from it, so recovery only ever has one file to trust.
   See [02](02-storage-engine.md).

3. **Why reads do not block each other.** Every file operation names the byte it
   acts on instead of moving a shared cursor, which is what lets a read borrow
   the file immutably. See [06](06-rust-notes.md).

4. **Why fsync is off by default.** A write already survives the broker dying.
   It does not survive the machine losing power, and closing that gap costs 366x
   throughput. See [04](04-broker-and-groups.md).
