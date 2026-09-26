# 6. The Rust this project leans on

Explanations of the language features doing real work here. If a piece of the
code looks like noise, it is probably explained below.

## `&self` versus `&mut self`, and why it decided the architecture

This is the single most important Rust idea in the project.

Rust allows either **many shared borrows** (`&T`) or **one exclusive borrow**
(`&mut T`), never both. That rule is enforced at compile time, and it is what
makes data races impossible rather than merely unlikely.

Now look at the obvious way to read from a file:

```rust
fn read(&mut self, offset: u64) -> Result<Record> {
    self.file.seek(SeekFrom::Start(position))?;
    self.file.read_exact(&mut buf)?;
}
```

This needs `&mut self` because seeking **moves a cursor**, which is mutation.
Two readers cannot hold `&mut` at once, so every read serializes behind every
other read even though reading changes no data.

Positional I/O removes the cursor:

```rust
fn read(&self, offset: u64) -> Result<Record> {
    read_exact_at(&self.log, &mut buf, position)?;
}
```

`pread` takes the byte offset as an argument. Nothing is mutated, so `&self` is
enough, so any number of readers can borrow at once.

That single change is what makes this possible:

```rust
Arc<RwLock<Partition>>
```

A `RwLock` is only worth having if readers genuinely take `&self`. With the
cursor based version, `RwLock` would have been a `Mutex` wearing a disguise.

**The chain:** positional I/O → `&self` reads → `RwLock` with real shared
reads → consumers do not block each other. Working backwards from "I want
concurrent reads" to "therefore no shared cursor" is the kind of reasoning the
borrow checker forces on you, and it is why the README claims what it claims.

## `Arc`, and why it is everywhere

`Arc<T>` is **A**tomically **R**eference **C**ounted. It is a pointer that
tracks how many owners exist and frees the value when the last one goes away.

Rust normally demands exactly one owner. `Arc` is how you say "several places
own this, free it when they are all done", safely across threads.

```rust
let broker = Arc::clone(&self.broker);
tokio::spawn(async move { handle_connection(socket, broker).await });
```

`Arc::clone` copies the pointer and bumps a counter. It does not copy the
broker. Every connection task gets its own handle to the same broker.

`Arc` alone gives shared **reading**. To mutate, you pair it with a lock:
`Arc<RwLock<Partition>>` means shared ownership plus controlled mutation.

## `RwLock` and lock poisoning

```rust
let guard = handle.read().unwrap_or_else(|e| e.into_inner());
```

The `unwrap_or_else` looks odd. It handles **poisoning**.

If a thread panics while holding a lock, Rust marks the lock poisoned, on the
theory that the data may have been left half updated. Every later `lock()`
returns `Err` forever after.

Here that default is too harsh. `Partition` only updates its state after a
successful write, so its invariants hold even if a thread panicked mid call.
`into_inner()` takes the data anyway. The alternative is a partition that is
permanently unusable because one unrelated request panicked once.

The guard releases automatically when it goes out of scope. There is no
`unlock()` to forget.

## `spawn_blocking`

```rust
spawn_blocking(move || {
    let mut guard = handle.write().unwrap_or_else(|e| e.into_inner());
    guard.append(&value)
}).await
```

Tokio runs thousands of async tasks on a handful of OS threads. That works
because tasks yield at every `.await`, letting the thread pick up another task.

A blocking file write never yields. It occupies its thread until the disk
responds, and every other task queued on that thread waits. One slow disk write
stalls unrelated connections.

`spawn_blocking` moves the work to a separate pool sized for blocking calls.
The async task awaits the result and yields in the meantime.

**Rule of thumb:** if it touches a file, a socket synchronously, or sleeps, it
does not belong on an async worker thread.

## `?` and `From`

```rust
let file = File::open(path)?;
```

`?` means "if this is an `Err`, return it from this function; otherwise unwrap
it". It replaces explicit match-and-return everywhere.

It also converts. `File::open` returns `io::Error` but our functions return
`Result<T, Error>`, and this is why that compiles:

```rust
impl From<io::Error> for Error {
    fn from(e: io::Error) -> Self {
        Error::Io(e)
    }
}
```

`?` calls `From::from` on the error automatically. Writing those `From` impls
once is what buys clean `?` everywhere else. It is also why this project needs
no `thiserror`: the derive macro generates exactly these impls, and by hand it
is about 20 lines.

## Pattern matching with `matches!` and let-else

```rust
assert!(matches!(segment.read(9), Err(Error::OffsetOutOfRange { .. })));
```

`matches!` asks "does this value fit this shape", returning a bool. `..` means
"and any other fields, I do not care".

```rust
let Some(name) = path.file_name().and_then(|n| n.to_str()) else {
    continue;
};
```

Let-else: bind `name` if the pattern matches, otherwise run the `else` block,
which must diverge (`continue`, `return`, `break`, `panic!`). It keeps the happy
path unindented instead of growing a pyramid of `if let`.

## Traits as shared behaviour

```rust
pub async fn read_frame<R>(reader: &mut R) -> Result<Option<BytesMut>>
where
    R: AsyncRead + Unpin,
```

Rather than demanding a `TcpStream`, this accepts anything that can be read
asynchronously. That is why the codec tests run against an in-memory
`Cursor<Vec<u8>>` with no socket involved, while production passes a real
`TcpStream`. Same function, no mocking framework.

`Unpin` is a technical requirement about whether a value can be moved in memory
after being polled. Treat it as boilerplate.

## serde derive

```rust
#[derive(Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum Request { ... }
```

`derive` generates code at compile time. `Serialize` writes the struct to JSON,
`Deserialize` reads it back, and `tag = "type"` says to store the enum variant
name in a field called `type`.

The whole wire protocol is that one attribute plus the enum definition. No
hand written parser, and no chance of the encoder and decoder disagreeing.

## `impl Trait` in return position

```rust
async fn spawn_blocking<T, F>(f: F) -> Result<T>
where
    F: FnOnce() -> Result<T> + Send + 'static,
```

`FnOnce() -> Result<T>` is "a closure that can be called once and returns a
Result". `Send` means it is safe to move to another thread. `'static` means it
borrows nothing that could expire, which is required because the closure may
outlive the function that created it.

That `'static` is why `produce` clones the `Arc` and moves an owned `Vec<u8>`
into the closure rather than passing a `&[u8]`. A borrow could dangle; an owned
value cannot.

## Things that look like comments but are not

Since the code has no comments, everything left is load bearing:

| Syntax | Meaning |
|---|---|
| `#[derive(...)]` | Generate trait implementations |
| `#[cfg(test)]` | Only compile this when testing |
| `#[cfg(unix)]` | Only compile this on Unix |
| `#[tokio::test]` | An async test, needs a runtime |
| `#[serde(tag = "type")]` | Change how serde encodes this |
| `_ = something` | Deliberately ignore this value |
| `let _guard = ...` | Keep alive until scope ends, do not warn |

## Where to learn more

- [The Rust Book](https://doc.rust-lang.org/book/), chapters 4 (ownership),
  15 (`Arc`, smart pointers) and 16 (concurrency) cover most of the above.
- [Tokio tutorial](https://tokio.rs/tokio/tutorial) for the async model.
- `cargo clippy` is a genuinely good teacher. It explains why, not just what.
