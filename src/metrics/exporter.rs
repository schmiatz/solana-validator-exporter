use crate::solana;
use crate::solana::validator::StakeState;
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
use tokio::sync::Mutex;

#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct MethodLabels {
    // pub epoch: u64,
    pub vote_account: String,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct StakeLabels {
    pub stake_type: String,
    pub vote_account: String,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct BlockLabels {
    pub block_type: String,
    pub vote_account: String,
}

pub struct Metrics {
    rpc_url: String,
    identity_account: String,
    vote_account: String,
    pub slot: Family<MethodLabels, Gauge>,
    pub epoch: Family<MethodLabels, Gauge>,
    pub epoch_progress: Family<MethodLabels, Gauge>,
    pub stake: Family<StakeLabels, Gauge>,
    pub identity_balance: Family<MethodLabels, Gauge>,
    pub vote_account_balance: Family<MethodLabels, Gauge>,
    pub blocks: Family<BlockLabels, Gauge>,
    pub jito_tips: Family<MethodLabels, Gauge>,
    pub vote_credit_rank: Family<MethodLabels, Gauge>,
    pub usd_price: Family<MethodLabels, Gauge>,
    pub epoch_block_rewards: Family<MethodLabels, Gauge>,
    pub ms_to_next_slot: Family<MethodLabels, Gauge>,
    pub last_block_rewards: Family<MethodLabels, Gauge>,
}

impl Metrics {
    pub fn new(rpc_url: String, identity_account: String, vote_account: String) -> Metrics {
        Metrics {
            rpc_url,
            identity_account,
            vote_account,
            slot: Family::default(),
            epoch: Family::default(),
            epoch_progress: Family::default(),
            stake: Family::default(),
            identity_balance: Family::default(),
            vote_account_balance: Family::default(),
            blocks: Family::default(),
            jito_tips: Family::default(),
            vote_credit_rank: Family::default(),
            usd_price: Family::default(),
            epoch_block_rewards: Family::default(),
            ms_to_next_slot: Family::default(),
            last_block_rewards: Family::default(),
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

        state.registry.register(
            "solana_epoch_progress",
            "Epoch progress",
            self.epoch_progress.clone(),
        );

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

        state.registry.register(
            "solana_last_block_rewards",
            "Average of last non-zero block rewards",
            self.last_block_rewards.clone(),
        );

        state
    }

    pub fn run_loop(self: Arc<Self>) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            let client = solana::validator::SolanaClient::new(
                &self.rpc_url,
                &self.identity_account,
                &self.vote_account,
            );

            // Create a channel for communicating current slot, epoch, and leader slots to background task
            let (slot_tx, mut slot_rx) = tokio::sync::mpsc::unbounded_channel::<(u64, u64, Vec<u64>)>();
            
            // Spawn background task for block rewards fetching
            let mut bg_client = solana::validator::SolanaClient::new(
                &self.rpc_url,
                &self.identity_account,
                &self.vote_account,
            );
            let bg_self = self.clone();
            tokio::spawn(async move {
                while let Some((current_slot, current_epoch, leader_slots)) = slot_rx.recv().await {
                    if !leader_slots.is_empty() {
                        match bg_client.get_block_rewards_sum(current_slot, current_epoch, leader_slots).await {
                            Ok(block_rewards) => {
                                bg_self.set_epoch_block_rewards(block_rewards);
                                
                                match bg_client.get_last_block_rewards().await {
                                    Ok(last_rewards) => {
                                        bg_self.set_last_block_rewards(last_rewards);
                                    }
                                    Err(e) => {
                                        error!("Error fetching last block rewards: {}", e);
                                    }
                                }
                            }
                            Err(e) => {
                                error!("Error fetching block rewards: {}", e);
                            }
                        }
                    }
                }
            });

            loop {
                let slot = match client.get_slot().await {
                    Ok(s) => Some(s),
                    Err(e) => {
                        error!("Error fetching slot: {}", e);
                        None
                    }
                };

                if let Some(slot) = slot {
                    self.set_slot(slot);
                }

                let epoch_info = match client.get_epoch().await {
                    Ok(e) => Some(e),
                    Err(e) => {
                        error!("Error fetching epoch: {}", e);
                        None
                    }
                };

                if let Some(epoch_info) = epoch_info {
                    let (epoch, epoch_progress) = epoch_info;
                    self.set_epoch(epoch);
                    self.set_epoch_progress(epoch_progress);
                }

                let stake = match client.get_stake_details().await {
                    Ok(e) => Some(e),
                    Err(e) => {
                        error!("Error fetching stake_details: {}", e);
                        None
                    }
                };

                if let Some(stake) = stake {
                    self.set_stake(stake);
                }

                let identity_balance = match client.get_identity_balance().await {
                    Ok(e) => Some(e),
                    Err(e) => {
                        error!("Error fetching identity balance: {}", e);
                        None
                    }
                };

                if let Some(identity_balance) = identity_balance {
                    self.set_identity_balance(identity_balance);
                }

                let vote_account_balance = match client.get_vote_balance().await {
                    Ok(e) => Some(e),
                    Err(e) => {
                        error!("Error fetching vote account: {}", e);
                        None
                    }
                };

                if let Some(vote_account_balance) = vote_account_balance {
                    self.set_vote_account_balance(vote_account_balance);
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
                    self.set_ms_to_next_slot(ms_to_next_slot);
                }

                if let (Some(leader_slots), Some((total, produced))) =
                    (leader_slots.as_ref(), block_production)
                {
                    self.set_block_production(
                        leader_slots.len() as u64,
                        produced as u64,
                        (total - produced) as u64,
                    );
                }

                let jito_tips = if let Some((epoch, _)) = epoch_info {
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
                    self.set_jito_tips(jito_tips);
                }

                let vote_credit_rank = match client.get_vote_credit_rank().await {
                    Ok(e) => Some(e),
                    Err(e) => {
                        error!("Error fetching vote credit rank: {}", e);
                        None
                    }
                };

                if let Some(vote_credit_rank) = vote_credit_rank {
                    self.set_vote_credit_rank(vote_credit_rank);
                }

                let usd_price = match client.get_sol_usd_price().await {
                    Ok(e) => Some(e),
                    Err(e) => {
                        error!("Error fetching SOL/USD: {}", e);
                        None
                    }
                };

                if let Some(usd_price) = usd_price {
                    self.set_usd_price(usd_price);
                }

                // Send current slot and leader slots to background task for block rewards processing
                if let (Some(slot), Some(leader_slots), Some(epoch_info)) = (slot, leader_slots.as_ref(), epoch_info.as_ref()) {
                    let (epoch, _) = *epoch_info;
                    if let Err(_) = slot_tx.send((slot, epoch as u64, leader_slots.clone())) {
                        error!("Failed to send slot info to background block rewards task");
                    }
                }

                // Add a small sleep to prevent overwhelming the RPC in the main loop
                tokio::time::sleep(tokio::time::Duration::from_millis(1000)).await;
            }
        })
    }

    pub fn set_slot(&self, slot: u64) {
        self.slot
            .get_or_create(&MethodLabels {
                vote_account: self.vote_account.clone(),
            })
            .set(slot as i64);
    }

    pub fn set_epoch(&self, epoch: i64) {
        self.epoch
            .get_or_create(&MethodLabels {
                vote_account: self.vote_account.clone(),
            })
            .set(epoch);
    }

    pub fn set_epoch_progress(&self, progress: i64) {
        self.epoch_progress
            .get_or_create(&MethodLabels {
                vote_account: self.vote_account.clone(),
            })
            .set(progress);
    }

    pub fn set_stake(&self, stake_state: StakeState) {
        self.stake
            .get_or_create(&StakeLabels {
                stake_type: "activated_stake".to_string(),
                vote_account: self.vote_account.clone(),
            })
            .set(stake_state.activated_stake as i64);

        self.stake
            .get_or_create(&StakeLabels {
                stake_type: "activating_stake".to_string(),
                vote_account: self.vote_account.clone(),
            })
            .set(stake_state.activating_stake as i64);

        self.stake
            .get_or_create(&StakeLabels {
                stake_type: "deactivating_stake".to_string(),
                vote_account: self.vote_account.clone(),
            })
            .set(stake_state.deactivating_stake as i64);

        self.stake
            .get_or_create(&StakeLabels {
                stake_type: "locked_stake".to_string(),
                vote_account: self.vote_account.clone(),
            })
            .set(stake_state.locked_stake as i64);

        self.stake
            .get_or_create(&StakeLabels {
                stake_type: "activated_stake_accounts".to_string(),
                vote_account: self.vote_account.clone(),
            })
            .set(stake_state.activated_stake_accounts as i64);

        self.stake
            .get_or_create(&StakeLabels {
                stake_type: "activating_stake_accounts".to_string(),
                vote_account: self.vote_account.clone(),
            })
            .set(stake_state.activating_stake_accounts as i64);

        self.stake
            .get_or_create(&StakeLabels {
                stake_type: "deactivating_stake_accounts".to_string(),
                vote_account: self.vote_account.clone(),
            })
            .set(stake_state.deactivating_stake_accounts as i64);
    }

    pub fn set_identity_balance(&self, balance: u64) {
        self.identity_balance
            .get_or_create(&MethodLabels {
                vote_account: self.vote_account.clone(),
            })
            .set(balance as i64);
    }

    pub fn set_vote_account_balance(&self, balance: u64) {
        self.vote_account_balance
            .get_or_create(&MethodLabels {
                vote_account: self.vote_account.clone(),
            })
            .set(balance as i64);
    }

    pub fn set_block_production(&self, total: u64, produced: u64, skipped: u64) {
        self.blocks
            .get_or_create(&BlockLabels {
                block_type: "total".to_string(),
                vote_account: self.vote_account.clone(),
            })
            .set(total as i64);

        self.blocks
            .get_or_create(&BlockLabels {
                block_type: "produced".to_string(),
                vote_account: self.vote_account.clone(),
            })
            .set(produced as i64);
        self.blocks
            .get_or_create(&BlockLabels {
                block_type: "skipped".to_string(),
                vote_account: self.vote_account.clone(),
            })
            .set(skipped as i64);
    }

    pub fn set_jito_tips(&self, tips: u64) {
        self.jito_tips
            .get_or_create(&MethodLabels {
                vote_account: self.vote_account.clone(),
            })
            .set(tips as i64);
    }

    pub fn set_vote_credit_rank(&self, rank: u32) {
        self.vote_credit_rank
            .get_or_create(&MethodLabels {
                vote_account: self.vote_account.clone(),
            })
            .set(rank as i64);
    }

    pub fn set_usd_price(&self, price: i64) {
        self.usd_price
            .get_or_create(&MethodLabels {
                vote_account: self.vote_account.clone(),
            })
            .set(price);
    }

    pub fn set_epoch_block_rewards(&self, block_rewards: i64) {
        self.epoch_block_rewards
            .get_or_create(&MethodLabels {
                vote_account: self.vote_account.clone(),
            })
            .set(block_rewards);
    }

    pub fn set_ms_to_next_slot(&self, ms_to_next_slot: i64) {
        self.ms_to_next_slot
            .get_or_create(&MethodLabels {
                vote_account: self.vote_account.clone(),
            })
            .set(ms_to_next_slot);
    }

    pub fn set_last_block_rewards(&self, last_block_rewards: i64) {
        self.last_block_rewards
            .get_or_create(&MethodLabels {
                vote_account: self.vote_account.clone(),
            })
            .set(last_block_rewards);
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
