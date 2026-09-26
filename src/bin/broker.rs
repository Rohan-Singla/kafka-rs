use std::path::PathBuf;
use std::process;
use std::sync::Arc;
use std::time::Duration;

use kafka_rust::broker::{Broker, BrokerConfig};
use kafka_rust::network::Server;

const USAGE: &str = "\
mini-kafka broker

USAGE:
    broker [OPTIONS]

OPTIONS:
    --addr <ADDR>          Address to listen on        [default: 127.0.0.1:9092]
    --data-dir <PATH>      Where segments are stored   [default: data]
    --segment-size <MB>    Roll to a new segment past this size [default: 64]
    --session-timeout <S>  Evict a silent consumer after this   [default: 30]
    --fsync                Fsync every write before acknowledging it
    -h, --help             Print this help
";

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let args: Vec<String> = std::env::args().skip(1).collect();
    let (addr, config) = match parse_args(&args) {
        Ok(Some(parsed)) => parsed,
        Ok(None) => {
            print!("{}", USAGE);
            return;
        }
        Err(e) => {
            eprintln!("error: {}\n\n{}", e, USAGE);
            process::exit(2);
        }
    };

    let broker = match Broker::open(config) {
        Ok(broker) => Arc::new(broker),
        Err(e) => {
            eprintln!("failed to open the broker: {}", e);
            process::exit(1);
        }
    };

    let server = match Server::bind(Arc::clone(&broker), &addr).await {
        Ok(server) => server,
        Err(e) => {
            eprintln!("failed to bind {}: {}", addr, e);
            process::exit(1);
        }
    };

    let bound = server
        .local_addr()
        .map(|a| a.to_string())
        .unwrap_or_else(|_| addr.clone());
    tracing::info!(
        "data dir {}, fsync {}",
        broker.config().data_dir.display(),
        broker.config().fsync
    );

    tokio::select! {
        result = server.run() => {
            if let Err(e) = result {
                eprintln!("server stopped: {}", e);
                process::exit(1);
            }
        }
        _ = tokio::signal::ctrl_c() => {
            tracing::info!("shutting down {}", bound);
        }
    }
}

type Parsed = (String, BrokerConfig);

fn parse_args(args: &[String]) -> Result<Option<Parsed>, String> {
    let mut addr = "127.0.0.1:9092".to_string();
    let mut config = BrokerConfig::new(PathBuf::from("data"));
    let mut i = 0;

    while i < args.len() {
        match args[i].as_str() {
            "-h" | "--help" => return Ok(None),
            "--fsync" => config.fsync = true,
            "--addr" => {
                addr = take(args, &mut i, "--addr")?;
            }
            "--data-dir" => {
                config.data_dir = PathBuf::from(take(args, &mut i, "--data-dir")?);
            }
            "--segment-size" => {
                let mb: u64 = take(args, &mut i, "--segment-size")?
                    .parse()
                    .map_err(|_| "--segment-size expects a number of megabytes".to_string())?;
                if mb == 0 {
                    return Err("--segment-size must be at least 1".to_string());
                }
                config.segment_size = mb
                    .checked_mul(1024 * 1024)
                    .ok_or_else(|| format!("--segment-size {} megabytes is too large", mb))?;
            }
            "--session-timeout" => {
                let secs: u64 = take(args, &mut i, "--session-timeout")?
                    .parse()
                    .map_err(|_| "--session-timeout expects a number of seconds".to_string())?;
                if secs == 0 {
                    return Err("--session-timeout must be at least 1".to_string());
                }
                config.session_timeout = Duration::from_secs(secs);
            }
            other => return Err(format!("unknown option '{}'", other)),
        }
        i += 1;
    }

    Ok(Some((addr, config)))
}

fn take(args: &[String], i: &mut usize, flag: &str) -> Result<String, String> {
    *i += 1;
    args.get(*i)
        .cloned()
        .ok_or_else(|| format!("{} needs a value", flag))
}
