mod metrics;
mod solana;
use axum::routing::get;
use axum::Router;
use clap::Parser;
use env_logger::Env;
use log::info;
use metrics::exporter::metrics_handler;
use serde::Deserialize;
use std::fs::File;
use std::io::BufReader;
use std::sync::Arc;
use tokio::sync::Mutex;

/// Prometheus exporter for solana validators
#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct CliArgs {
    /// Path to config file
    #[arg(long)]
    config_file: String,
}

#[derive(Debug, Deserialize)]
struct Config {
    rpc_url: String,
    port: i32,
    vote_account: String,
    identity_account: String,
}
#[tokio::main]
async fn main() {
    let args = CliArgs::parse();
    let file = File::open(args.config_file).unwrap();
    let reader = BufReader::new(file);
    env_logger::Builder::from_env(Env::default().default_filter_or("info")).init();

    // Parse the YAML file
    let config: Config = serde_yaml::from_reader(reader).expect("Error parsing yaml file");

    info!("Starting exporter!");
    let metrics = Arc::new(metrics::exporter::Metrics::new(
        config.rpc_url,
        config.identity_account,
        config.vote_account,
    ));
    let state = metrics.init_state();
    let state_mut = Arc::new(Mutex::new(state));
    let _handle = metrics.clone().run_loop();
    let router = Router::new()
        .route("/metrics", get(metrics_handler))
        .with_state(state_mut);
    let listener = tokio::net::TcpListener::bind(format!("0.0.0.0:{}", config.port))
        .await
        .unwrap();

    axum::serve(listener, router).await.unwrap();
}
