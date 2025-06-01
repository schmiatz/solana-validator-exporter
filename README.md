# Solana Validator Exporter

This is a Prometheus exporter for Solana validators.

## Setup

1.  **Create a configuration file.**

    Copy the `config.example.yaml` to `config.yaml` and update the values:
    ```bash
    cp config.example.yaml config.yaml
    ```
    Then, edit `config.yaml`:

    *   `rpc_url`: The RPC URL for the Solana cluster (e.g., `https://api.mainnet-beta.solana.com`).
    *   `port`: The port the exporter will listen on (e.g., `9090`).
    *   `vote_account`: Your validator's vote account public key.
    *   `identity_account`: Your validator's identity account public key.

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