use crate::solana;
use crate::solana::orangefin::StakeState;
use axum::body::Body;
use axum::extract::State;
use axum::http::header::CONTENT_TYPE;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use log::error;
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
    pub vote_credit_rank: Family<MethodLabels, Gauge>,
    pub usd_price: Family<MethodLabels, Gauge>,
    pub epoch_block_rewards: Family<MethodLabels, Gauge>,
    pub ms_to_next_slot: Family<MethodLabels, Gauge>,
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
            vote_credit_rank: Family::default(),
            usd_price: Family::default(),
            epoch_block_rewards: Family::default(),
            ms_to_next_slot: Family::default(),
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
            "Vote account balance",
            self.vote_account_balance.clone(),
        );

        state
            .registry
            .register("solana_blocks", "Block production", self.blocks.clone());

        state
            .registry
            .register("solana_jito_tips", "Jito tips", self.jito_tips.clone());

        state.registry.register(
            "solana_vote_credit_rank",
            "Vote credit rank",
            self.vote_credit_rank.clone(),
        );

        state
            .registry
            .register("solana_usd_price", "USD Price", self.usd_price.clone());

        state.registry.register(
            "solana_epoch_block_rewards",
            "Sum of block rewards this epoch",
            self.epoch_block_rewards.clone(),
        );

        state.registry.register(
            "solana_ms_to_next_slot",
            "Time to next leader slot",
            self.ms_to_next_slot.clone(),
        );

        state
    }

    pub async fn run_loop(&self) {
        let metrics = Arc::new(Mutex::new(self));

        let mut client = solana::orangefin::SolanaClient::new(
            &self.rpc_url,
            &self.identity_account,
            &self.vote_account,
        );
        loop {
            let slot = match client.get_slot().await {
                Ok(s) => Some(s),
                Err(e) => {
                    error!("Error fetching slot: {}", e);
                    None
                }
            };

            if let Some(slot) = slot {
                metrics.lock().await.set_slot(slot);
            }

            let epoch = match client.get_epoch().await {
                Ok(e) => Some(e),
                Err(e) => {
                    error!("Error fetching epoch: {}", e);
                    None
                }
            };

            if let Some(epoch) = epoch {
                metrics.lock().await.set_epoch(epoch);
            }

            let stake = match client.get_stake_details().await {
                Ok(e) => Some(e),
                Err(e) => {
                    error!("Error fetching stake_details: {}", e);
                    None
                }
            };

            if let Some(stake) = stake {
                metrics.lock().await.set_stake(stake);
            }

            let identity_balance = match client.get_identity_balance().await {
                Ok(e) => Some(e),
                Err(e) => {
                    error!("Error fetching identity balance: {}", e);
                    None
                }
            };

            if let Some(identity_balance) = identity_balance {
                metrics.lock().await.set_identity_balance(identity_balance);
            }

            let vote_account_balance = match client.get_vote_balance().await {
                Ok(e) => Some(e),
                Err(e) => {
                    error!("Error fetching vote account: {}", e);
                    None
                }
            };

            if let Some(vote_account_balance) = vote_account_balance {
                metrics
                    .lock()
                    .await
                    .set_vote_account_balance(vote_account_balance);
            }

            let leader_slots = match client.get_leader_info().await {
                Ok(mut e) => {
                    e.sort();
                    Some(e)
                }
                Err(e) => {
                    error!("Error fetching leader slots: {}", e);
                    Some(vec![])
                }
            };

            let block_production = match client.get_block_production().await {
                Ok(e) => Some(e),
                Err(e) => {
                    error!("Error fetching block production: {}", e);
                    None
                }
            };

            let ms_to_next_slot =
                if let (Some(slot), Some(leader_slots)) = (slot, leader_slots.clone()) {
                    match client.get_ms_to_next_slot(slot, leader_slots).await {
                        Ok(e) => Some(e),
                        Err(e) => {
                            error!("Error fetching time to next slot: {}", e);
                            None
                        }
                    }
                } else {
                    None
                };

            if let Some(ms_to_next_slot) = ms_to_next_slot {
                metrics.lock().await.set_ms_to_next_slot(ms_to_next_slot);
            }

            if let (Some(leader_slots), Some((total, produced))) =
                (leader_slots.as_ref(), block_production)
            {
                metrics.lock().await.set_block_production(
                    leader_slots.len() as u64,
                    produced as u64,
                    (total - produced) as u64,
                );
            }

            let jito_tips = if let Some(epoch) = epoch {
                match client.get_jito_tips(epoch).await {
                    Ok(e) => Some(e),
                    Err(e) => {
                        error!("Error fetching jito tips: {}", e);
                        None
                    }
                }
            } else {
                None
            };

            if let Some(jito_tips) = jito_tips {
                metrics.lock().await.set_jito_tips(jito_tips);
            }

            let vote_credit_rank = match client.get_vote_credit_rank().await {
                Ok(e) => Some(e),
                Err(e) => {
                    error!("Error fetching vote credit rank: {}", e);
                    None
                }
            };

            if let Some(vote_credit_rank) = vote_credit_rank {
                metrics.lock().await.set_vote_credit_rank(vote_credit_rank);
            }

            let usd_price = match client.get_sol_usd_price().await {
                Ok(e) => Some(e),
                Err(e) => {
                    error!("Error fetching SOL/USD: {}", e);
                    None
                }
            };

            if let Some(usd_price) = usd_price {
                metrics.lock().await.set_usd_price(usd_price);
            }

            let block_rewards = if let Some((slot, leader_slots)) = slot.zip(leader_slots.as_ref())
            {
                match client
                    .get_block_rewards_sum(slot, leader_slots.clone())
                    .await
                {
                    Ok(e) => Some(e),
                    Err(e) => {
                        error!("Error fetching block rewards: {}", e);
                        None
                    }
                }
            } else {
                None
            };

            if let Some(block_rewards) = block_rewards {
                metrics.lock().await.set_epoch_block_rewards(block_rewards);
            }
            sleep(Duration::from_secs(60)).await;
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

    pub fn set_vote_credit_rank(&self, rank: u32) {
        self.vote_credit_rank
            .get_or_create(&MethodLabels {})
            .set(rank as i64);
    }

    pub fn set_usd_price(&self, price: i64) {
        self.usd_price.get_or_create(&MethodLabels {}).set(price);
    }

    pub fn set_epoch_block_rewards(&self, block_rewards: i64) {
        self.epoch_block_rewards
            .get_or_create(&MethodLabels {})
            .set(block_rewards);
    }

    pub fn set_ms_to_next_slot(&self, ms_to_next_slot: i64) {
        self.ms_to_next_slot
            .get_or_create(&MethodLabels {})
            .set(ms_to_next_slot);
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
