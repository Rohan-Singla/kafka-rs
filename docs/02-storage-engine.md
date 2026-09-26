# 2. The storage engine

This is the heart of the project. Files: `src/storage/`.

## The record on disk

Every message becomes a 24 byte header followed by the payload.

```
 0        8         16      20      24
 +--------+---------+-------+-------+------------------+
 | offset |timestamp|  crc  |length | payload          |
 | u64    | u64     | u32   | u32   | length bytes     |
 +--------+---------+-------+-------+------------------+
```

All integers are big endian, so a hex dump reads left to right in the order you
would write the number by hand.

Writing `hi` as offset 0 produces 26 bytes:

```
00 00 00 00 00 00 00 00    offset   = 0
00 00 01 92 4F 3B A1 00    timestamp = milliseconds since epoch
A1 B4 09 3C                crc32
00 00 00 02                length   = 2
68 69                      payload  = "hi"
```

Code: `record::encode` in `src/storage/record.rs`.

### Why the checksum covers more than the payload

```rust
pub fn checksum(offset: u64, timestamp: u64, value: &[u8]) -> u32
```

It hashes the offset, timestamp and length *as well as* the payload. If it only
covered the payload, a corrupted `length` field would still look valid, and the
reader would trust it and read the wrong number of bytes. Covering the framing
fields means any corruption anywhere in the record is caught.

This matters more than it sounds. It is what makes recovery able to tell "this
record is fine" from "this record is garbage", which is the whole basis of the
crash handling below.

## The index

The `.idx` file is a flat array of fixed size entries. No headers, no padding:

```
 0        8        16       24       32
 +--------+--------+--------+--------+
 |offset 0|  pos 0 |offset 1|  pos 1 | ...
 +--------+--------+--------+--------+
    u64      u64      u64      u64
```

Every entry is exactly **16 bytes**. That constant is the entire trick.

To find offset N in a segment starting at `base_offset`:

```
index_position = (N - base_offset) * 16
```

Seek there, read 8 bytes past the stored offset, and you have the byte position
in the `.log`. No searching, no scanning, no tree. Arithmetic.

Finding offset 1,000,000 costs exactly as much as finding offset 5.

Code: `Segment::read` in `src/storage/segment.rs`.

### The tradeoff

One index entry per message means a million messages costs 16MB of index. That
is the price of making a lookup pure arithmetic. Real Kafka indexes only every
Nth message and scans the short gap between entries, spending a little read time
to carry a much smaller index.

### Sequential reads skip the index

`Segment::read_from` looks up the *first* offset in the index, then walks the
log forward. Records are laid out back to back, so once you know where one
record ends you know where the next begins:

```
next_position = current_position + 24 + length
```

That is why fetching 500 records is barely more expensive than fetching one, and
why the consume benchmark is an order of magnitude faster than produce.

### A batch is bounded by bytes, not just count

`read_from` takes both a `max_count` and a `max_bytes` budget, and stops at
whichever it hits first. Count alone is not enough: 10,000 records of 8MB each
is 80GB, which would be assembled in memory before anyone noticed.

One exception keeps a consumer from deadlocking. If the very first record is
larger than the whole budget, it is returned anyway. Otherwise a consumer whose
next message is bigger than the limit would fetch nothing, forever, and never
advance past it.

## Crash recovery

The important part.

### The rule

> The log is the source of truth. The index is a derived accelerator.

The index can always be rebuilt from the log. The log can never be rebuilt from
the index. So recovery only ever has to trust one file.

### What a crash actually leaves behind

`Segment::append` writes the log first, then the index. If the process dies in
between, you get a log record with no index entry. That ordering is deliberate:
the index ends up *short*, which is harmless and fixable. The reverse order
would leave the index pointing at bytes that were never written, which is not.

Three things can be wrong at the tail after a crash:

1. **A half written header.** Fewer than 24 bytes at the end.
2. **A header with a missing payload.** `length` claims 200 bytes, 60 are there.
3. **A complete looking record with a bad checksum.** Silent corruption.

### The recovery pass

On open, `Segment::recover` replays the log from byte 0:

```
position = 0
expected = base_offset

loop:
    read 24 byte header at position
    if fewer than 24 bytes remain        -> stop
    if header.offset != expected         -> stop
    if header.length is implausible      -> stop
    if position + 24 + length > file_len -> stop
    read the payload
    if checksum does not match           -> stop

    remember position
    position += 24 + length
    expected += 1

truncate the log to position
rewrite the index from the remembered positions
```

Everything before the first bad record is kept. Everything from it onward is
dropped. The log is truncated to the last good boundary and the index is
rebuilt to match.

**A failed read is not a torn tail.** Every `stop` above is a judgement that the
bytes are bad, and the response to it is `set_len`, which destroys everything
after that point. A read that fails outright (EIO, a network filesystem
hiccup) says nothing about the bytes; it says the disk did not answer. Treating
the two the same would turn a transient error into permanent data loss, so a
read error aborts the open and leaves the file untouched. The length checks
before it are what make that distinction safe: running out of file is already
handled explicitly, so an error reaching the read really is a failure.

The offset continuity check (`header.offset != expected`) is doing real work
alongside the checksum: it catches a record that is internally valid but landed
in the wrong place, which is what the old version of this code produced when it
reopened a segment and restarted its offset counter.

### Seeing it work

Three tests cover the three failure modes, in `src/storage/segment.rs`:

| Test | Simulates |
|---|---|
| `recovery_truncates_a_torn_tail` | Crash partway through an append |
| `recovery_stops_at_a_flipped_bit` | Silent disk corruption |
| `index_is_rebuilt_from_the_log` | Index deleted entirely |

You can also do it by hand:

```bash
cargo run --release --bin broker --  --data-dir /tmp/demo   # produce some messages, then
kill -9 <pid>
cargo run --release --bin broker --  --data-dir /tmp/demo   # comes back, data intact
```

## Segments and rolling

`Partition` (`src/storage/partition.rs`) holds an ordered `Vec<Segment>`. Only
the last one is written to.

```rust
pub fn append(&mut self, value: &[u8]) -> Result<u64> {
    if self.active().size() >= self.max_segment_size {
        let base_offset = self.active().next_offset();
        self.segments.push(Segment::open(&self.dir, base_offset)?);
    }
    self.active_mut().append(value)
}
```

Finding which segment holds an offset is a binary search over base offsets, in
`Partition::segment_for`. Reads cross segment boundaries transparently:
`read_from` keeps pulling from successive segments until it has enough records
or runs out of log.

Two edge cases worth knowing.

**An empty trailing segment.** A crash right after rolling can leave one.
`Partition::with_segment_size` deletes those on open, so the active segment is
always the one holding the highest offset.

**A hole in the middle.** Segments recover independently, so corruption in a
sealed middle segment truncates that segment and leaves a gap between it and
the next one. `read_from` detects this (the segment holding the offset reports
nothing, and the offset is past that segment's end), logs a warning, and
resumes at the next segment's base offset.

Without that, everything after the hole would be silently unreachable through
sequential reads while still being visible to point reads: the worst kind of
bug, because the data looks present but never arrives. Covered by
`a_hole_in_a_sealed_segment_does_not_hide_later_records`.

## Positional I/O

Every read and write names the byte it acts on:

```rust
read_exact_at(&self.log, &mut buf, position)
write_all_at(&self.log, &encoded, position)
```

rather than seek-then-read. `src/storage/fileio.rs` wraps the platform specific
calls (`pread`/`pwrite` on Unix, `seek_read`/`seek_write` on Windows).

This is not a micro-optimisation. It is the decision the whole concurrency story
rests on, and [06-rust-notes.md](06-rust-notes.md) explains why.

Next: [03-network-protocol.md](03-network-protocol.md).
