use ansi_term::Color::{Green, Red, White, Yellow};
use near_chain::{RuntimeAdapter, StoreValidator};
use near_chain_configs::GenesisValidationMode;
use nearcore::load_config;
use std::path::Path;
use std::process;
use std::sync::Arc;

pub fn run(home_dir: &Path) {
    let near_config = load_config(home_dir, GenesisValidationMode::Full)
        .unwrap_or_else(|e| panic!("Error loading config: {:#}", e));

    let store = near_store::StoreOpener::new(home_dir, &near_config.config.store).open();

    let runtime_adapter: Arc<dyn RuntimeAdapter> =
        Arc::new(nearcore::NightshadeRuntime::from_config(home_dir, store.clone(), &near_config));

    let mut store_validator = StoreValidator::new(
        near_config.validator_signer.as_ref().map(|x| x.validator_id().clone()),
        near_config.genesis.config,
        runtime_adapter.clone(),
        store,
        false,
    );
    store_validator.validate();

    if store_validator.tests_done() == 0 {
        println!("{}", Red.bold().paint("No conditions has been validated"));
        process::exit(1);
    }
    println!(
        "{} {}",
        White.bold().paint("Conditions validated:"),
        Green.bold().paint(store_validator.tests_done().to_string())
    );
    for error in store_validator.errors.iter() {
        println!(
            "{}  {}  {}",
            Red.bold().paint(&error.col),
            Yellow.bold().paint(&error.key),
            error.err
        );
    }
    if store_validator.is_failed() {
        println!("Errors found: {}", Red.bold().paint(store_validator.num_failed().to_string()));
        process::exit(1);
    } else {
        println!("{}", Green.bold().paint("No errors found"));
    }
    let gc_counters = store_validator.get_gc_counters();
    for (col, count) in gc_counters {
        println!("{} {}", White.bold().paint(col), count);
    }
}
