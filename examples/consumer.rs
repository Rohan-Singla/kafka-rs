use std::io::{Read, Write};
use std::net::TcpStream;
use serde_json::{json, Value};

fn send(stream: &mut TcpStream, request: Value) -> Value {
    let bytes = serde_json::to_vec(&request).unwrap();
    stream.write_all(&(bytes.len() as u32).to_be_bytes()).unwrap();
    stream.write_all(&bytes).unwrap();

    let mut len_buf = [0u8; 4];
    stream.read_exact(&mut len_buf).unwrap();
    let mut buf = vec![0u8; u32::from_be_bytes(len_buf) as usize];
    stream.read_exact(&mut buf).unwrap();
    serde_json::from_slice(&buf).unwrap()
}

fn main() {
    let mut stream = TcpStream::connect("127.0.0.1:9092").expect("could not connect — is the broker running?");

    let group     = "my-group";
    let topic     = "orders";
    let partition = 0u32;

    // find out where this consumer group left off
    let resp = send(&mut stream, json!({
        "type": "FetchOffset",
        "group": group,
        "topic": topic,
        "partition": partition
    }));
    let start_offset = resp["offset"].as_u64().unwrap_or(0);
    println!("resuming from offset {}", start_offset);

    // fetch up to 10 messages
    let resp = send(&mut stream, json!({
        "type": "Fetch",
        "topic": topic,
        "partition": partition,
        "offset": start_offset,
        "max_count": 10
    }));

    let empty = vec![];
    let messages = resp["messages"].as_array().unwrap_or(&empty);

    if messages.is_empty() {
        println!("no new messages");
        return;
    }

    let mut last_offset = start_offset;
    for msg in messages {
        let offset = msg["offset"].as_u64().unwrap_or(0);
        let value  = msg["value"].as_str().unwrap_or("");
        println!("offset {} → {}", offset, value);
        last_offset = offset;
    }

    // commit next offset so we don't re-read on the next run
    send(&mut stream, json!({
        "type": "CommitOffset",
        "group": group,
        "topic": topic,
        "partition": partition,
        "offset": last_offset + 1
    }));
    println!("committed offset {}", last_offset + 1);
}
