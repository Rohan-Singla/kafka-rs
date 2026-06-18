use std::{path::PathBuf, sync::Arc};
use kafka_rust::{broker::Broker, network};

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();

    let data_dir = PathBuf::from("data");
    let broker = Arc::new(Broker::new(data_dir).expect("failed to initialize broker"));

    network::run(broker, "127.0.0.1:9092")
        .await
        .expect("server crashed");
}
