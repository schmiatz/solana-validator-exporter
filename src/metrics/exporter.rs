use crate::solana;
use crate::solana::orangefin::StakeState;
use axum::body::Body;
use axum::extract::State;
use axum::http::header::CONTENT_TYPE;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use prometheus_client::encoding::text::encode;
use prometheus_client::encoding::EncodeLabelSet;
use prometheus_client::metrics::family::Family;
use prometheus_client::metrics::gauge::Gauge;
use prometheus_client::registry::Registry;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;
use tokio::time::sleep;

#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct MethodLabels {
    // pub epoch: u64,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct StakeLabels {
    pub stake_type: String,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct BlockLabels {
    pub block_type: String,
}

pub struct Metrics {
    rpc_url: String,
    identity_account: String,
    vote_account: String,
    pub slot: Family<MethodLabels, Gauge>,
    pub epoch: Family<MethodLabels, Gauge>,
    pub stake: Family<StakeLabels, Gauge>,
    pub identity_balance: Family<MethodLabels, Gauge>,
    pub vote_account_balance: Family<MethodLabels, Gauge>,
    pub blocks: Family<BlockLabels, Gauge>,
    pub jito_tips: Family<MethodLabels, Gauge>,
}

impl Metrics {
    pub fn new(rpc_url: String, identity_account: String, vote_account: String) -> Metrics {
        Metrics {
            rpc_url,
            identity_account,
            vote_account,
            slot: Family::default(),
            epoch: Family::default(),
            stake: Family::default(),
            identity_balance: Family::default(),
            vote_account_balance: Family::default(),
            blocks: Family::default(),
            jito_tips: Family::default(),
        }
    }

    pub fn init_state(&self) -> AppState {
        let mut state = AppState {
            registry: Registry::default(),
        };
        state
            .registry
            .register("solana_slot", "Slot of cluster", self.slot.clone());

        state
            .registry
            .register("solana_epoch", "Current epoch", self.epoch.clone());

        state
            .registry
            .register("solana_stake", "Stake info", self.stake.clone());

        state.registry.register(
            "solana_identity_balance",
            "Identity balance",
            self.identity_balance.clone(),
        );

        state.registry.register(
            "solana_vote_account_balance",
            "Bote account balance",
            self.vote_account_balance.clone(),
        );

        state
            .registry
            .register("solana_blocks", "Block production", self.blocks.clone());

        state
            .registry
            .register("solana_jito_tips", "Jito tips", self.jito_tips.clone());

        state
    }

    pub async fn run_loop(&self) {
        let metrics = Arc::new(Mutex::new(self));

        let client = solana::orangefin::SolanaClient::new(
            &self.rpc_url,
            &self.identity_account,
            &self.vote_account,
        );
        loop {
            let slot = client.get_slot().await.unwrap();
            let epoch = client.get_epoch().await.unwrap();
            let stake = client.get_stake_details().await;
            let identity_balance = client.get_identity_balance().await.unwrap();
            let vote_account_balance = client.get_vote_balance().await.unwrap();
            let leader_slots = client.get_leader_info().await.unwrap();
            let (total, produced) = client.get_block_production().await.unwrap();
            let jito_tips = client.get_jito_tips(epoch).await.unwrap();
            metrics.lock().await.set_slot(slot);
            metrics.lock().await.set_epoch(epoch);
            metrics.lock().await.set_stake(stake);
            metrics.lock().await.set_identity_balance(identity_balance);
            metrics
                .lock()
                .await
                .set_vote_account_balance(vote_account_balance);
            metrics.lock().await.set_block_production(
                leader_slots.len() as u64,
                produced as u64,
                (total - produced) as u64,
            );
            metrics.lock().await.set_jito_tips(jito_tips);
            sleep(Duration::from_secs(180)).await;
        }
    }

    pub fn set_slot(&self, slot: u64) {
        self.slot.get_or_create(&MethodLabels {}).set(slot as i64);
    }

    pub fn set_epoch(&self, epoch: u64) {
        self.epoch.get_or_create(&MethodLabels {}).set(epoch as i64);
    }

    pub fn set_stake(&self, stake_state: StakeState) {
        self.stake
            .get_or_create(&StakeLabels {
                stake_type: "activated_stake".to_string(),
            })
            .set(stake_state.activated_stake as i64);

        self.stake
            .get_or_create(&StakeLabels {
                stake_type: "activating_stake".to_string(),
            })
            .set(stake_state.activating_stake as i64);

        self.stake
            .get_or_create(&StakeLabels {
                stake_type: "deactivating_stake".to_string(),
            })
            .set(stake_state.deactivating_stake as i64);

        self.stake
            .get_or_create(&StakeLabels {
                stake_type: "locked_stake".to_string(),
            })
            .set(stake_state.locked_stake as i64);

        self.stake
            .get_or_create(&StakeLabels {
                stake_type: "activated_stake_accounts".to_string(),
            })
            .set(stake_state.activated_stake_accounts as i64);

        self.stake
            .get_or_create(&StakeLabels {
                stake_type: "activating_stake_accounts".to_string(),
            })
            .set(stake_state.activating_stake_accounts as i64);

        self.stake
            .get_or_create(&StakeLabels {
                stake_type: "deactivating_stake_accounts".to_string(),
            })
            .set(stake_state.deactivating_stake_accounts as i64);
    }

    pub fn set_identity_balance(&self, balance: u64) {
        self.identity_balance
            .get_or_create(&MethodLabels {})
            .set(balance as i64);
    }

    pub fn set_vote_account_balance(&self, balance: u64) {
        self.vote_account_balance
            .get_or_create(&MethodLabels {})
            .set(balance as i64);
    }

    pub fn set_block_production(&self, total: u64, produced: u64, skipped: u64) {
        self.blocks
            .get_or_create(&BlockLabels {
                block_type: "total".to_string(),
            })
            .set(total as i64);

        self.blocks
            .get_or_create(&BlockLabels {
                block_type: "produced".to_string(),
            })
            .set(produced as i64);
        self.blocks
            .get_or_create(&BlockLabels {
                block_type: "skipped".to_string(),
            })
            .set(skipped as i64);
    }

    pub fn set_jito_tips(&self, tips: u64) {
        self.jito_tips
            .get_or_create(&MethodLabels {})
            .set(tips as i64);
    }
}

pub struct AppState {
    pub registry: Registry,
}

pub async fn metrics_handler(State(state): State<Arc<Mutex<AppState>>>) -> impl IntoResponse {
    let state = state.lock().await;
    let mut buffer = String::new();
    encode(&mut buffer, &state.registry).unwrap();

    Response::builder()
        .status(StatusCode::OK)
        .header(
            CONTENT_TYPE,
            "application/openmetrics-text; version=1.0.0; charset=utf-8",
        )
        .body(Body::from(buffer))
        .unwrap()
}
