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
struct ValidatorEntry {
    vote_account: String,
    identity_account: String,
}

#[derive(Debug, Deserialize)]
struct Config {
    port: i32,
    // Network RPC URLs
    rpc_url_mainnet: Option<String>,
    rpc_url_testnet: Option<String>,
    rpc_url_devnet: Option<String>,
    
    // New multi-validator format
    mainnet_validators: Option<Vec<ValidatorEntry>>,
    testnet_validators: Option<Vec<ValidatorEntry>>,
    devnet_validators: Option<Vec<ValidatorEntry>>,
    
    // Legacy single-validator format (for backward compatibility)
    mainnet_vote_account: Option<String>,
    mainnet_identity_account: Option<String>,
    testnet_vote_account: Option<String>,
    testnet_identity_account: Option<String>,
    devnet_vote_account: Option<String>,
    devnet_identity_account: Option<String>,
}

#[derive(Debug, Clone)]
struct ValidatorConfig {
    network: String,
    rpc_url: String,
    vote_account: String,
    identity_account: String,
}

fn parse_validators_for_network(
    network: &str,
    rpc_url: Option<&String>,
    multi_validators: Option<&Vec<ValidatorEntry>>,
    legacy_vote: Option<&String>,
    legacy_identity: Option<&String>,
) -> Vec<ValidatorConfig> {
    let mut validators = Vec::new();
    
    if let Some(rpc_url) = rpc_url {
        // First, try the new multi-validator format
        if let Some(validator_list) = multi_validators {
            for validator in validator_list {
                validators.push(ValidatorConfig {
                    network: network.to_string(),
                    rpc_url: rpc_url.clone(),
                    vote_account: validator.vote_account.clone(),
                    identity_account: validator.identity_account.clone(),
                });
            }
        }
        // If no multi-validator config, fall back to legacy single-validator format
        else if let (Some(vote_account), Some(identity_account)) = (legacy_vote, legacy_identity) {
            validators.push(ValidatorConfig {
                network: network.to_string(),
                rpc_url: rpc_url.clone(),
                vote_account: vote_account.clone(),
                identity_account: identity_account.clone(),
            });
        }
    }
    
    validators
}

#[tokio::main]
async fn main() {
    let args = CliArgs::parse();
    let file = File::open(args.config_file).unwrap();
    let reader = BufReader::new(file);
    env_logger::Builder::from_env(Env::default().default_filter_or("info")).init();

    // Parse the YAML file
    let config: Config = serde_yaml::from_reader(reader).expect("Error parsing yaml file");

    // Build validator configs for all networks
    let mut validator_configs = Vec::new();

    // Parse mainnet validators
    let mainnet_validators = parse_validators_for_network(
        "mainnet",
        config.rpc_url_mainnet.as_ref(),
        config.mainnet_validators.as_ref(),
        config.mainnet_vote_account.as_ref(),
        config.mainnet_identity_account.as_ref(),
    );
    validator_configs.extend(mainnet_validators);

    // Parse testnet validators
    let testnet_validators = parse_validators_for_network(
        "testnet",
        config.rpc_url_testnet.as_ref(),
        config.testnet_validators.as_ref(),
        config.testnet_vote_account.as_ref(),
        config.testnet_identity_account.as_ref(),
    );
    validator_configs.extend(testnet_validators);

    // Parse devnet validators
    let devnet_validators = parse_validators_for_network(
        "devnet",
        config.rpc_url_devnet.as_ref(),
        config.devnet_validators.as_ref(),
        config.devnet_vote_account.as_ref(),
        config.devnet_identity_account.as_ref(),
    );
    validator_configs.extend(devnet_validators);

    if validator_configs.is_empty() {
        panic!("At least one complete validator configuration (RPC URL, vote account, and identity account) must be provided");
    }

    info!("Starting exporter with {} validator(s) across {} network(s)!", 
          validator_configs.len(),
          validator_configs.iter().map(|v| v.network.as_str()).collect::<std::collections::HashSet<_>>().len());
    
    // Log each validator being monitored
    for validator_config in &validator_configs {
        info!("Monitoring {} validator: {} (Identity: {})", 
              validator_config.network, 
              validator_config.vote_account,
              validator_config.identity_account);
    }
    
    // Create shared metrics registry
    let shared_state = Arc::new(Mutex::new(metrics::exporter::AppState {
        registry: prometheus_client::registry::Registry::default(),
    }));
    
    // Create and start metrics collection for each validator
    let mut handles = Vec::new();
    for validator_config in validator_configs {
        let metrics = Arc::new(metrics::exporter::Metrics::new(
            validator_config.network,
            validator_config.rpc_url,
            validator_config.identity_account,
            validator_config.vote_account,
        ));
        
        // Initialize metrics in shared registry
        metrics.init_registry(shared_state.clone()).await;
        
        // Start metrics collection loop
        let handle = metrics.clone().run_loop();
        handles.push(handle);
    }

    let router = Router::new()
        .route("/metrics", get(metrics_handler))
        .with_state(shared_state.clone());
    
    let listener = tokio::net::TcpListener::bind(format!("0.0.0.0:{}", config.port))
        .await
        .unwrap();

    axum::serve(listener, router).await.unwrap();
}
