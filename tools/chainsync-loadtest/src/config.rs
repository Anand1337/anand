use near_chain_configs::Genesis;
use nearcore::config;
use nearcore::config::NearConfig;
use near_primitives::hash::CryptoHash;
use near_primitives::block::{GenesisId};

fn genesis_hash(chain_id: &str) -> CryptoHash {
    return match chain_id {
        "mainnet" => "EPnLgE7iEq9s7yTkos96M3cWymH5avBAPm3qx3NXqR8H",
        "testnet" => "FWJ9kR6KFWoyMoNjpLXXGHeuiy7tEY6GmoFeCA5yuc6b",
        "betanet" => "6hy7VoEJhPEUaJr1d5ePBhKdgeDWKCjLoUAn7XS9YPj",
        _ => {
            return Default::default();
        }
    }
    .parse()
    .unwrap();
}

pub fn genesis_id(chain_id: &str) -> GenesisId {
  GenesisId {
    chain_id: chain_id.to_string(),
    hash: genesis_hash(chain_id),
  }
}

pub fn download(chain_id: &str) -> anyhow::Result<NearConfig> {
    let mut dir = std::env::temp_dir();
    // dirs::cache_dir().context("dirs::cache_dir() = None")?;
    dir.push("near_configs");
    dir.push(&chain_id);
    let dir = dir.as_path();

    // Always fetch the config.
    std::fs::create_dir_all(dir)?;
    let url = config::get_config_url(chain_id);
    let config_path = &dir.join(config::CONFIG_FILENAME);
    config::download_config(&url, config_path)?;
    let config = config::Config::from_file(config_path)?;

    // Generate node key.
    let account_id = "node".parse().unwrap();
    let node_signer =
        near_crypto::InMemorySigner::from_random(account_id, near_crypto::KeyType::ED25519);
    let mut genesis = Genesis::default();
    genesis.config.chain_id = chain_id.to_string();
    return Ok(NearConfig::new(config, genesis, (&node_signer).into(), None));
}
