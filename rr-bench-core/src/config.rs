use clap::Parser;
use std::collections::HashMap;
use std::time::Duration;

/// Configuration struct used to store key-value pairs passed as flags to the benchmark.
pub struct Config(HashMap<String, String>);

impl Config {
    pub fn get(&self, key: &str) -> Option<&str> {
        self.0.get(key).map(|v| v.as_str())
    }
}

#[derive(Parser, Debug, Clone)]
#[command(name = "rr-bench")]
pub struct Cli {
    /// The duration of the benchmark (e.g., 10s, 5m, 1h)
    #[arg(long, value_parser = parse_duration_str)]
    pub duration: Duration,

    /// The number of transactions per second
    /// to execute against the primary database.
    #[arg(long, default_value_t = 10)]
    pub transactions_per_second: u32,

    /// Any flags passed at the end after --
    /// will be based to the benchmark as a config.
    /// This may be used for implementation specific
    /// configurations.
    #[arg(last = true)]
    unknown_args: Vec<String>,
}

impl Cli {
    pub fn get_config(&self) -> Config {
        let mut iter = self.unknown_args.iter();
        let mut unknown_flags = HashMap::new();
        while let Some(arg) = iter.next() {
            if let Some((key, value)) = arg.split_once('=') {
                unknown_flags.insert(key.to_string(), value.to_string());
            } else {
                // Handle flags without a value (e.g., --flag)
                let flag = arg.trim_start_matches("--");
                if let Some(value) = iter.next() {
                    unknown_flags.insert(flag.to_string(), value.clone());
                }
            }
        }

        Config(unknown_flags)
    }
}

fn parse_duration_str(s: &str) -> Result<Duration, String> {
    humantime::parse_duration(s)
        .map_err(|_| format!("Invalid duration {}. Use formats like '10s', '5m', '1h'", s))
}
