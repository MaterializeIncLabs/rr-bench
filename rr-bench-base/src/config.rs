use clap::{Arg, ArgMatches, Command, value_parser};
use std::time::Duration;

pub struct Args {
    command: Command,
}

impl Args {
    pub fn new(args: impl IntoIterator<Item = impl Into<Arg>>) -> Self {
        let command = Command::new("rr-bench")
            .arg(
                Arg::new("duration")
                    .short('d')
                    .long("duration")
                    .help("The duration of the benchmark (e.g., 10s, 5m, 1h)")
                    .value_name("DURATION")
                    .required(true)
                    .value_parser(parse_duration)
            )
            .arg(
                Arg::new("transactions_per_second")
                    .long("transactions-per-second")
                    .help(
                        "The number of transactions per second to execute against the primary database",
                    )
                    .value_name("TPS")
                    .default_value("10")
                    .value_parser(value_parser!(u32)),
            )
            .arg(
                Arg::new("concurrency")
                    .short('c')
                    .long("concurrency")
                    .help("The number of concurrent clients to open against the read replica")
                    .value_name("CONCURRENCY")
                    .default_value("1")
                    .value_parser(value_parser!(u32)),
            )
            .args(args);

        Self { command }
    }

    pub fn parse(self) -> Cli {
        let matches = self.command.get_matches();
        let duration = *matches.get_one::<Duration>("duration").unwrap();
        let transactions_per_second = *matches.get_one::<u32>("transactions_per_second").unwrap();
        let concurrency = *matches.get_one::<u32>("concurrency").unwrap();

        Cli {
            duration,
            transactions_per_second,
            concurrency,
            matches,
        }
    }
}

fn parse_duration(s: &str) -> Result<Duration, String> {
    humantime::parse_duration(s)
        .map_err(|_| format!("Invalid duration {}. Use formats like '10s', '5m', '1h'", s))
}

pub struct Cli {
    pub duration: Duration,
    pub transactions_per_second: u32,
    pub concurrency: u32,
    pub matches: ArgMatches,
}
