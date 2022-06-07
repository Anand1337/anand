use std::path::Path;
use clap::{Arg, Command};
use near_logger_utils::init_integration_logger;
use nearcore::get_default_home;
use store_validator::run;

fn main() {
    init_integration_logger();

    let default_home = get_default_home();
    let matches = Command::new("store-validator")
        .arg(
            Arg::new("home")
                .long("home")
                .default_value_os(default_home.as_os_str())
                .help("Directory for config and data (default \"~/.near\")")
                .takes_value(true),
        )
        .subcommand(Command::new("validate"))
        .get_matches();

    let home_dir = matches.value_of("home").map(Path::new).unwrap();
    run(home_dir);
}
