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

    // create topic with 2 partitions
    let resp = send(&mut stream, json!({
        "type": "CreateTopic",
        "name": "orders",
        "partitions": 2
    }));
    println!("create topic → {:?}", resp);

    // send 5 messages to partition 0
    for i in 1..=5 {
        let resp = send(&mut stream, json!({
            "type": "Produce",
            "topic": "orders",
            "partition": 0,
            "message": format!("order #{}", i)
        }));
        println!("produced → {:?}", resp);
    }
}
