# Solana Validator Exporter

This is a Prometheus exporter for Solana validators that supports monitoring multiple validators across different networks (mainnet, testnet, devnet).

## Features

- **Multi-Network Support**: Monitor validators on mainnet, testnet, and devnet simultaneously
- **Multi-Validator Support**: Monitor **multiple validators per network** (e.g., 2 on mainnet, 3 on testnet, 1 on devnet)
- **Flexible Configuration**: Both list-based and legacy single-validator configuration formats
- **Comprehensive Metrics**: Track validator performance, financial metrics, and network statistics
- **Network Labels**: All metrics include network and vote account labels for easy identification
- **Backward Compatibility**: Existing single-validator configurations continue to work

## Setup

1.  **Create a configuration file.**

    Copy the `config.example.yaml` to `config.yaml` and update the values:
    ```bash
    cp config.example.yaml config.yaml
    ```
    Then, edit `config.yaml`:

    ### Global Configuration
    *   `port`: The port the exporter will listen on (e.g., `9090`).

    ### Network RPC URLs (at least one required)
    *   `rpc_url_mainnet`: The RPC URL for Solana mainnet (e.g., `https://api.mainnet-beta.solana.com`).
    *   `rpc_url_testnet`: The RPC URL for Solana testnet (optional).
    *   `rpc_url_devnet`: The RPC URL for Solana devnet (optional).

    ### Multi-Validator Configuration (Recommended)
    You can now define multiple validators per network:
    
    ```yaml
    mainnet_validators:
      - vote_account: "YOUR_FIRST_MAINNET_VOTE_ACCOUNT"
        identity_account: "YOUR_FIRST_MAINNET_IDENTITY_ACCOUNT"
      - vote_account: "YOUR_SECOND_MAINNET_VOTE_ACCOUNT"
        identity_account: "YOUR_SECOND_MAINNET_IDENTITY_ACCOUNT"
    
    testnet_validators:
      - vote_account: "YOUR_FIRST_TESTNET_VOTE_ACCOUNT"
        identity_account: "YOUR_FIRST_TESTNET_IDENTITY_ACCOUNT"
      - vote_account: "YOUR_SECOND_TESTNET_VOTE_ACCOUNT"
        identity_account: "YOUR_SECOND_TESTNET_IDENTITY_ACCOUNT"
      - vote_account: "YOUR_THIRD_TESTNET_VOTE_ACCOUNT"
        identity_account: "YOUR_THIRD_TESTNET_IDENTITY_ACCOUNT"
    ```

    ### Legacy Single-Validator Configuration (Still Supported)
    For backward compatibility, you can still use the single-validator format:
    *   `mainnet_vote_account` & `mainnet_identity_account`: Your mainnet validator's accounts.
    *   `testnet_vote_account` & `testnet_identity_account`: Your testnet validator's accounts.
    *   `devnet_vote_account` & `devnet_identity_account`: Your devnet validator's accounts.

    **Note**: The multi-validator format takes precedence over the legacy format if both are defined.

## Running the Exporter

You have two options to run the exporter: direct build or Docker container.

### Option 1: Direct Build

1. **Install Rust** (if not already installed):
   ```bash
   curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
   ```

2. **Build the exporter**:
   ```bash
   cargo build --release
   ```

3. **Run the exporter**:
   ```bash
   ./target/release/solana-validator-exporter --config-file config.yaml
   ```

### Option 2: Docker Container

1. **Build the Docker image**:
   ```bash
   docker build -t solana-validator-exporter -f docker/Dockerfile .
   ```

2. **Run the container**:
   ```bash
   docker run -d \
     --name solana-validator-exporter \
     -p 9090:9090 \
     -v $(pwd)/config.yaml:/home/exporter/config.yaml \
     solana-validator-exporter --config-file /home/exporter/config.yaml
   ```

## Verifying the Exporter

After running either method, you can verify the exporter is working by accessing the metrics endpoint:
```bash
curl http://localhost:9090/metrics
```

The exporter will expose metrics on the port specified in your configuration file (e.g., `http://localhost:9090/metrics`).

## Metrics Labels

All metrics now include the following labels for easy filtering and identification:
- `network`: The Solana network (mainnet, testnet, or devnet)
- `vote_account`: The validator's vote account public key

Example metrics with multiple validators:
```
# Mainnet validators
solana_slot{network="mainnet",vote_account="FirstMainnetVoteAccount..."} 123456789
solana_slot{network="mainnet",vote_account="SecondMainnetVoteAccount..."} 123456789

# Testnet validators  
solana_slot{network="testnet",vote_account="FirstTestnetVoteAccount..."} 987654321
solana_slot{network="testnet",vote_account="SecondTestnetVoteAccount..."} 987654321
solana_slot{network="testnet",vote_account="ThirdTestnetVoteAccount..."} 987654321
```

This allows you to create separate Grafana dashboards or alerts for different networks and validators, or aggregate metrics across multiple validators within the same network.