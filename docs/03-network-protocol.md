# 3. The network protocol

Files: `src/network/`.

## Framing: where does one message end?

TCP is a stream of bytes with no message boundaries. Send `{"a":1}` then
`{"b":2}` and the receiver might get both in one read, or the first one split
across two reads. TCP guarantees order and delivery, not chunking.

So every message is prefixed with its length:

```
+----------+---------------------------+
| length   | payload                   |
| u32 BE   | exactly `length` bytes    |
+----------+---------------------------+
```

Read 4 bytes, decode the length, read exactly that many more. Now you have one
complete message regardless of how TCP chopped it up.

Code: `read_frame` and `write_frame` in `src/network/codec.rs`.

### The size cap exists for a reason

```rust
pub const MAX_FRAME_SIZE: usize = 16 * 1024 * 1024;
```

Without it, this line is a denial of service:

```rust
let mut payload = BytesMut::zeroed(length);
```

A client sends four bytes saying `0xFFFFFFFF` and the broker tries to allocate
4GB before a single byte of body has arrived. Four bytes of input, gigabytes of
memory. The cap is checked *before* the allocation.

There is a test for exactly this: `an_oversized_frame_is_refused` in
`tests/integration.rs` sends `u32::MAX` and asserts the broker refuses it and
keeps serving other clients.

### One write, not two

`write_frame` builds the length prefix and payload into one buffer and issues a
single `write_all`. Two separate writes would let a reader observe a header with
no body behind it, and would cost an extra syscall per response.

## The payload: tagged JSON

```rust
#[derive(Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum Request { ... }
```

`#[serde(tag = "type")]` puts the variant name into a `type` field, so a frame
is self describing:

```json
{"type":"Produce","topic":"orders","partition":0,"message":"hello"}
```

serde reads `type`, picks the matching variant, and fills in the fields. An
unknown `type` is a parse error, not a panic.

**This is a deliberate tradeoff.** JSON is slower than a binary encoding and
that shows in the benchmarks. It buys debuggability: you can drive the broker
from a shell script and read a packet capture without tooling. Framing is a
separate module from payload encoding, so swapping JSON for something binary
touches one file.

One consequence worth knowing: message payloads are `String`, so they are
UTF-8 at the protocol level, even though the storage engine underneath is
happily byte oriented.

## Every request

Defined in `src/network/protocol.rs`.

| Request | Fields | Response |
|---|---|---|
| `CreateTopic` | name, partitions | `Ok` |
| `Produce` | topic, partition, message | `Offset` |
| `Fetch` | topic, partition, offset, max_count | `Messages` |
| `CommitOffset` | group, topic, partition, offset | `Ok` |
| `FetchOffset` | group, topic, partition | `Offset` |
| `ListTopics` | | `Topics` |
| `DescribeTopic` | topic | `Topic` |
| `JoinGroup` | group, topics | `Assignment` |
| `Heartbeat` | group, member_id | `Assignment` |
| `LeaveGroup` | group, member_id | `Ok` |
| `ListGroups` | | `Groups` |
| `DescribeGroup` | group | `Group` |

Any of them can come back as `{"type":"Error","reason":"..."}` instead.

## Errors do not kill the connection

```rust
Request::Produce { .. } => match broker.produce(...).await {
    Ok(offset) => Response::Offset { offset },
    Err(e) => e.into(),      // becomes Response::Error
},
```

An unknown topic, a bad partition, an invalid name: all of these come back as a
normal response and the socket stays open. Only a framing level failure (an
oversized frame, a truncated body) drops the connection, because at that point
the stream position is no longer trustworthy.

`errors_come_back_without_killing_the_connection` in `tests/integration.rs`
fires four bad requests down one socket and then asserts a good one still works.

## The server loop

```rust
loop {
    let (socket, peer) = listener.accept().await?;
    tokio::spawn(async move { handle_connection(socket, broker).await });
}
```

Every connection gets its own task. A task is a few kilobytes of heap, versus
roughly a megabyte of stack for an OS thread, so idle connections are close to
free and ten thousand of them stay manageable.

Two details in there:

- **A failed `accept` does not kill the broker.** Hitting a file descriptor
  limit or a peer hanging up mid handshake logs a warning and continues.
- **`set_nodelay(true)`** disables Nagle's algorithm. Nagle waits to coalesce
  small writes into bigger packets, which is great for bulk transfer and
  terrible for a request/response protocol where the next request depends on
  this reply.

## Driving it by hand

The protocol is simple enough to speak from a script. This sends `ListTopics`:

```python
import socket, struct, json
s = socket.create_connection(("127.0.0.1", 9092))
body = json.dumps({"type": "ListTopics"}).encode()
s.sendall(struct.pack(">I", len(body)) + body)
n = struct.unpack(">I", s.recv(4))[0]
print(json.loads(s.recv(n)))
```

Being able to do that in eleven lines, in another language, is the point of
choosing a readable protocol.

Next: [04-broker-and-groups.md](04-broker-and-groups.md).
