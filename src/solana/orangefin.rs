use std::str::FromStr;

use log::{debug, info};
use serde::Deserialize;
use solana_rpc_client::nonblocking::rpc_client::RpcClient;
use solana_rpc_client_api::config::{
    RpcAccountInfoConfig, RpcBlockConfig, RpcGetVoteAccountsConfig, RpcProgramAccountsConfig,
};
use solana_rpc_client_api::filter::{Memcmp, RpcFilterType};
use solana_rpc_client_api::response::RpcPerfSample;
use solana_sdk::account_utils::StateMut;
use solana_sdk::pubkey::Pubkey;
use solana_sdk::stake;
use solana_sdk::stake::state::StakeStateV2;
use solana_sdk::sysvar;
use solana_sdk::sysvar::clock::Clock;
use solana_sdk::sysvar::stake_history;
use std::collections::HashMap;
use std::fmt;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

pub struct SolanaClient {
    client: RpcClient,
    vote_account: String,
    identity_account: String,
    block_rewards: HashMap<u64, i64>,
}

pub struct StakeState {
    pub activated_stake: u64,
    pub activating_stake: u64,
    pub deactivating_stake: u64,
    pub locked_stake: u64,
    pub activated_stake_accounts: u64,
    pub activating_stake_accounts: u64,
    pub deactivating_stake_accounts: u64,
}

#[derive(Debug, Deserialize)]
struct KrakenResponse {
    result: ResultData,
}

#[derive(Debug, Deserialize)]
#[allow(non_snake_case)]
struct ResultData {
    SOLUSD: TickerData,
}

#[derive(Debug, Deserialize)]
struct TickerData {
    c: [String; 2], // Array for the closing price and volume
}

impl fmt::Display for StakeState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Format each stake field by dividing by 1e9
        write!(
            f,
            "StakeState {{\n \
            Activated Stake: {:.9},\n \
            Activating Stake: {:.9},\n \
            Deactivating Stake: {:.9},\n \
            Locked Stake: {:.9},\n \
            Activated Stake Accounts: {},\n \
            Activating Stake Accounts: {},\n \
            Deactivating Stake Accounts: {}\n\
            }}",
            self.activated_stake as f64 / 1e9,
            self.activating_stake as f64 / 1e9,
            self.deactivating_stake as f64 / 1e9,
            self.locked_stake as f64 / 1e9,
            self.activated_stake_accounts,
            self.activating_stake_accounts,
            self.deactivating_stake_accounts,
        )
    }
}

impl SolanaClient {
    pub fn new(url: &str, identity_account: &str, vote_account: &str) -> SolanaClient {
        let client = RpcClient::new_with_timeout(url.to_string(), Duration::from_secs(30));
        SolanaClient {
            client,
            vote_account: vote_account.to_string(),
            identity_account: identity_account.to_string(),
            block_rewards: HashMap::new(),
        }
    }

    pub async fn get_slot(&self) -> Result<u64, Box<dyn std::error::Error>> {
        let slot = self.client.get_slot().await?;
        Ok(slot)
    }

    pub async fn get_epoch(&self) -> Result<u64, Box<dyn std::error::Error>> {
        let epoch = self.client.get_epoch_info().await?;
        Ok(epoch.epoch)
    }

    pub async fn get_stake_details(&self) -> Result<StakeState, Box<dyn std::error::Error>> {
        let program_accounts_config = RpcProgramAccountsConfig {
            account_config: RpcAccountInfoConfig {
                encoding: Some(solana_account_decoder::UiAccountEncoding::Base64),
                ..RpcAccountInfoConfig::default()
            },
            filters: Some(vec![
                // Filter by `StakeStateV2::Stake(_, _)`
                RpcFilterType::Memcmp(Memcmp::new_base58_encoded(0, &[2, 0, 0, 0])),
                // Filter by `Delegation::voter_pubkey`, which begins at byte offset 124
                RpcFilterType::Memcmp(Memcmp::new_base58_encoded(
                    124,
                    &Pubkey::from_str(&self.vote_account)?.to_bytes(),
                )),
            ]),
            ..RpcProgramAccountsConfig::default()
        };
        let mut stake_details = StakeState {
            activated_stake: 0,
            activating_stake: 0,
            deactivating_stake: 0,
            locked_stake: 0,
            activated_stake_accounts: 0,
            activating_stake_accounts: 0,
            deactivating_stake_accounts: 0,
        };
        let stake_accounts = self
            .client
            .get_program_accounts_with_config(&stake::program::id(), program_accounts_config)
            .await?;
        let clock_account = self.client.get_account(&sysvar::clock::id()).await?;
        let stake_history_account = self.client.get_account(&stake_history::id()).await?;
        let clock: Clock = solana_sdk::account::from_account(&clock_account)
            .ok_or_else(|| "Error parsing clock account")?;
        let stake_history: solana_sdk::stake_history::StakeHistory =
            solana_sdk::account::from_account(&stake_history_account)
                .ok_or_else(|| "Error parsing stake_history_account")?;
        let start = SystemTime::now();
        let since_the_epoch = start
            .duration_since(UNIX_EPOCH)
            .expect("Time went backwards");
        let unix_timestamp = since_the_epoch.as_secs() as i64;
        for (stake_pubkey, stake_account) in stake_accounts {
            let stake_state: StakeStateV2 = stake_account.state()?;
            match stake_state {
                StakeStateV2::Initialized(_) => {
                    println!("pubkey: {:?}", stake_pubkey);
                }
                StakeStateV2::Stake(_, stake, _) => {
                    let account = stake.delegation.stake_activating_and_deactivating(
                        clock.epoch,
                        &stake_history,
                        Some(520), // Todo: hack but it's not relevant anymore
                    );
                    let meta = stake_state
                        .meta()
                        .ok_or_else(|| "Error parsing stake account meta info")?;
                    if meta.lockup.custodian
                        != Pubkey::from_str("11111111111111111111111111111111")?
                        && meta.lockup.unix_timestamp > unix_timestamp
                        && account.effective > 0
                    {
                        stake_details.locked_stake += account.effective;
                    }
                    stake_details.activated_stake += account.effective;
                    stake_details.activating_stake += account.activating;
                    stake_details.deactivating_stake += account.deactivating;

                    if account.effective > 0 {
                        stake_details.activated_stake_accounts += 1;
                    } else if account.activating > 0 {
                        stake_details.activating_stake_accounts += 1;
                    }
                    if account.deactivating > 0 {
                        stake_details.deactivating_stake_accounts += 1;
                    }
                }
                _ => {}
            }
        }
        Ok(stake_details)
    }
    pub async fn get_identity_balance(&self) -> Result<u64, Box<dyn std::error::Error>> {
        let balance = self
            .client
            .get_balance(&Pubkey::from_str(&self.identity_account)?)
            .await?;
        Ok(balance)
    }
    pub async fn get_vote_balance(&self) -> Result<u64, Box<dyn std::error::Error>> {
        let balance = self
            .client
            .get_balance(&Pubkey::from_str(&self.vote_account)?)
            .await?;
        Ok(balance)
    }
    pub async fn get_leader_info(&self) -> Result<Vec<u64>, Box<dyn std::error::Error>> {
        let epoch_info = self.client.get_epoch_info().await?;
        let epoch_schedule = self.client.get_epoch_schedule().await?;
        let first_slot_in_epoch = epoch_schedule.get_first_slot_in_epoch(epoch_info.epoch);
        let leader_schedule = self
            .client
            .get_leader_schedule(Some(first_slot_in_epoch))
            .await?
            .ok_or_else(|| "Error parsing leader schedule")?;

        let my_leader_slots = leader_schedule
            .iter()
            .filter(|(pubkey, _)| **pubkey == self.identity_account)
            .next();

        let mut leader_slots: Vec<u64> = Vec::new();
        for (_, slots) in my_leader_slots.iter() {
            for slot_index in slots.iter() {
                leader_slots.push(*slot_index as u64 + first_slot_in_epoch)
            }
        }
        Ok(leader_slots)
    }

    pub async fn get_block_production(&self) -> Result<(usize, usize), Box<dyn std::error::Error>> {
        let blocks = self.client.get_block_production().await?;
        let my_blocks = blocks
            .value
            .by_identity
            .iter()
            .filter(|(identity, _)| **identity == self.identity_account)
            .next();
        let (_, block_production) =
            my_blocks.ok_or_else(|| "Error parsing leader block production")?;
        Ok(*block_production)
    }

    pub async fn get_block_rewards(&self, slot: u64) -> Result<i64, Box<dyn std::error::Error>> {
        match self
            .client
            .get_block_with_config(
                slot,
                RpcBlockConfig {
                    transaction_details: None,
                    rewards: Some(true),
                    max_supported_transaction_version: Some(0),
                    ..RpcBlockConfig::default()
                },
            )
            .await
        {
            Ok(block) => Ok(block.rewards.ok_or_else(|| "Error fetching rewards")?[0].lamports),
            Err(e) => {
                let error = e.to_string();
                if error.contains("skipped") {
                    Ok(0)
                } else {
                    Err(Box::new(e))
                }
            }
        }
    }

    pub async fn get_block_rewards_sum(
        &mut self,
        current_slot: u64,
        leader_slots: Vec<u64>,
    ) -> Result<i64, Box<dyn std::error::Error>> {
        for slot in leader_slots.iter() {
            // New epoch, reset
            if let Some(first_slot) = self.block_rewards.keys().min() {
                if leader_slots[0] != *first_slot {
                    info!("New epoch detected");
                    self.block_rewards.clear();
                }
            }
            // If we already fetched the leader slot, skip to next.
            if self.block_rewards.contains_key(slot) {
                continue;
            }
            // If slot is in the future, break out and skip the rest.
            if *slot > current_slot {
                break;
            }
            info!("Fetching block rewards for slot {}", slot);
            let block_rewards = self.get_block_rewards(*slot).await?;
            self.block_rewards.insert(*slot, block_rewards);
        }
        let sum: i64 = self.block_rewards.values().sum();
        Ok(sum)
    }

    pub async fn get_jito_tips(&self, epoch: u64) -> Result<u64, Box<dyn std::error::Error>> {
        let (addr, _) = Pubkey::find_program_address(
            &[
                b"TIP_DISTRIBUTION_ACCOUNT",
                Pubkey::from_str("oRAnGeU5h8h2UkvbfnE5cjXnnAa4rBoaxmS4kbFymSe")?
                    .to_bytes()
                    .as_ref(),
                epoch.to_le_bytes().as_ref(),
            ],
            &Pubkey::from_str("4R3gSG8BpU4t19KYj8CfnbtRpnT8gtk4dvTHxVRwc2r7")?,
        );
        let balance = self.client.get_balance(&addr).await?;
        Ok(balance)
    }

    pub async fn get_vote_credit_rank(&self) -> Result<u32, Box<dyn std::error::Error>> {
        let vote_accounts = self
            .client
            .get_vote_accounts_with_config(RpcGetVoteAccountsConfig {
                keep_unstaked_delinquents: Some(false),
                delinquent_slot_distance: Some(125),
                ..RpcGetVoteAccountsConfig::default()
            })
            .await?;
        let mut validator_credits: Vec<_> = vote_accounts
            .current
            .iter()
            .map(|vote_account| {
                let current_epoch = &vote_account.epoch_credits.last();
                if let Some((_, credits, prev_credits)) = current_epoch {
                    (
                        vote_account.vote_pubkey.clone(),
                        credits.saturating_sub(*prev_credits),
                    )
                } else {
                    (vote_account.vote_pubkey.clone(), 0)
                }
            })
            .collect();
        validator_credits.sort_by(|a, b| b.1.cmp(&a.1));
        let mut place = 0;
        for (index, (vote_pubkey, _)) in validator_credits.iter().enumerate() {
            if *vote_pubkey == *self.vote_account {
                place = (index + 1) as u32;
                break;
            }
        }
        Ok(place)
    }

    pub async fn get_sol_usd_price(&self) -> Result<i64, Box<dyn std::error::Error>> {
        debug!("Fetching sol price from kraken");
        let resp = reqwest::get("https://api.kraken.com/0/public/Ticker?pair=SOLUSD").await?;
        let data: KrakenResponse = resp.json().await?;
        debug!("Kraken response: {:?}", data);
        let price_float: f64 = data.result.SOLUSD.c[0].parse()?;
        Ok((price_float * 1e5) as i64)
    }

    pub async fn get_ms_to_next_slot(
        &self,
        current_slot: u64,
        leader_slots: Vec<u64>,
    ) -> Result<i64, Box<dyn std::error::Error>> {
        let samples = self.client.get_recent_performance_samples(Some(60)).await?;

        let (slots, secs) = samples.iter().fold(
            (0, 0u64),
            |(slots, secs): (u64, u64),
             RpcPerfSample {
                 num_slots,
                 sample_period_secs,
                 ..
             }| {
                (
                    slots.saturating_add(*num_slots),
                    secs.saturating_add((*sample_period_secs).into()),
                )
            },
        );
        let average_slot_time_ms = secs.saturating_mul(1000).checked_div(slots).unwrap_or(400);
        let mut next_slot: u64 = 0;
        for slot in leader_slots.iter() {
            if *slot > current_slot {
                next_slot = *slot;
                break;
            }
        }
        Ok(((next_slot - current_slot) * average_slot_time_ms) as i64)
    }
}
