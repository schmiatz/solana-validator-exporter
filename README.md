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

2.  **Build the exporter.**
    ```bash
    cargo build --release
    ```

## Usage

Run the exporter with the following command:

```bash
./target/release/solana-validator-exporter --config-file config.yaml
```

The exporter will then expose metrics on the port specified in your configuration file (e.g., `http://localhost:9090/metrics`).