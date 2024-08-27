#![allow(clippy::needless_doctest_main)]

use crate::config::Cli;
pub use crate::config::Config;
use crate::measurements::Measurements;
use crate::primary_simulator::PrimarySimulator;
use crate::read_simulator::simulate_reader_connection;
use anyhow::{Context, Result};
use clap::Parser;
use indicatif::{ProgressBar, ProgressStyle};
use std::process::exit;
use std::sync::mpsc;
use std::sync::mpsc::RecvTimeoutError;
use std::thread;
use std::time::{Duration, Instant};

mod config;
mod measurements;
mod primary_simulator;
mod read_simulator;

/// The `Benchmark` trait defines the interface for setting up a database benchmarking environment.
/// Implementors of this trait are responsible for providing access to both the primary database
/// and the read replica.
pub trait Benchmark: Send {
    type Writer: PrimaryDatabase + 'static;

    type Reader: ReadReplica + 'static;

    /// Provides access to the primary database. This
    /// method may be called multiple times.
    fn primary_database(&self) -> Result<Self::Writer>;

    /// Provides access to the read replica. This
    /// method may be called multiple times and should
    /// return a handle with a new connection each time.
    fn read_replica(&self) -> Result<Self::Reader>;
}

/// The `PrimaryDatabase` trait defines the interface for interacting with the primary database
/// in a benchmarking environment. This trait includes methods for retrieving random IDs from
/// various tables and executing operations such as inserts, updates, or deletes.
pub trait PrimaryDatabase: Send {
    fn get_random_customer_id(&self) -> Result<u64>;

    fn get_random_account_id(&self) -> Result<u64>;

    fn get_random_security_id(&self) -> Result<u64>;

    fn get_random_trade_id(&self) -> Result<u64>;

    fn get_random_order_id(&self) -> Result<u64>;

    fn get_random_market_data_id(&self) -> Result<u64>;

    fn get_random_sector(&self) -> Result<String>;

    fn execute_command(&self, cmds: Operation) -> Result<()>;
}

/// The `ReadReplica` trait defines the interface for interacting with a read replica
/// in a benchmarking environment. This trait includes methods for executing various
/// read operations that are typical in OLTP systems, such as fetching customer portfolios
/// or querying market data.
pub trait ReadReplica: Send {
    fn customer_portfolio(&self, customer_id: u64) -> Result<()>;

    fn top_performers(&self) -> Result<()>;

    fn market_overview(&self) -> Result<()>;

    fn recent_large_trades(&self) -> Result<()>;

    fn customer_order_book(&self, customer_id: u64) -> Result<()>;

    fn sector_performance(&self, sector: String) -> Result<()>;

    fn account_activity_summary(&self, account_id: u64) -> Result<()>;

    fn daily_market_movements(&self, security_id: u64) -> Result<()>;

    fn high_value_customers(&self) -> Result<()>;

    fn pending_orders_summary(&self) -> Result<()>;

    fn trade_volume_by_hour(&self) -> Result<()>;

    fn top_securities_by_sector(&self, sector: String) -> Result<()>;

    fn recent_trades_by_account(&self, account_id: u64) -> Result<()>;

    fn order_fulfillment_rates(&self, customer_id: u64) -> Result<()>;

    fn sector_order_activity(&self, sector: String) -> Result<()>;

    fn cascading_order_cancellation_alert(&self) -> Result<()>;
}

pub enum Operation {
    InsertCustomer {
        name: String,
        address: String,
    },
    InsertAccount {
        customer_id: u64,
        account_type: String,
        balance: f64,
        parent_account_id: Option<u64>,
    },
    InsertSecurity {
        ticker: String,
        name: String,
        sector: String,
    },
    InsertTrade {
        account_id: u64,
        security_id: u64,
        trade_type: String,
        quantity: i32,
        price: f64,
        parent_trade_id: Option<u64>,
    },
    InsertOrder {
        account_id: u64,
        security_id: u64,
        order_type: String,
        quantity: i32,
        limit_price: f64,
        status: String,
        parent_order_id: Option<u64>,
    },
    InsertMarketData {
        security_id: u64,
        price: f64,
        volume: i32,
    },

    UpdateCustomer {
        customer_id: u64,
        address: String,
    },
    UpdateAccount {
        account_id: u64,
        balance: f64,
    },
    UpdateTrade {
        trade_id: u64,
        price: f64,
    },
    UpdateOrder {
        order_id: u64,
        status: String,
        limit_price: f64,
    },
    UpdateMarketData {
        market_data_id: u64,
        price: f64,
        volume: f64,
    },

    DeleteCustomer {
        customer_id: u64,
    },
    DeleteAccount {
        account_id: u64,
    },
    DeleteSecurity {
        security_id: u64,
    },
    DeleteTrade {
        trade_id: u64,
    },
    DeleteOrder {
        order_id: u64,
    },
    DeleteMarketData {
        market_data_id: u64,
    },
}

/// The `benchmark` function runs a benchmarking test using the provided function to create a `Benchmark` instance.
///
/// This function sets up the benchmarking environment, spawns threads to simulate primary database writes,
/// and read replica queries, and collects measurements over the specified duration.
///
/// # Arguments
/// * `f` - A function that takes a `Config` and returns a `Benchmark` implementation.
///
/// # Example
/// ```no_run
/// fn main() {
///     benchmark(|config| {
///         MyBenchmark::new(config)
///     });
/// }
/// ```
pub fn benchmark<B: Benchmark, F>(f: F)
where
    F: Fn(Config) -> B,
{
    match inner(f) {
        Ok(measurements) => println!("{}", measurements),
        Err(e) => {
            eprintln!("{:?}", e);
            exit(1)
        }
    }
}

fn inner<B: Benchmark, F>(f: F) -> Result<Measurements>
where
    F: Fn(Config) -> B,
{
    let cli: Cli = Cli::parse();

    let cli = cli.clone();
    let benchmark = f(cli.get_config());
    let primary = benchmark
        .primary_database()
        .context("failed to build primary database client")?;

    thread::spawn(move || {
        let mut simulator = PrimarySimulator::new(primary, cli.transactions_per_second, 42);
        if let Err(e) = simulator.run() {
            eprintln!("{:?}", e);
            exit(1)
        }
    });

    let (tx, rx) = mpsc::channel();

    let secondary = benchmark
        .primary_database()
        .context("failed to build primary database client")?;

    let reader = benchmark
        .read_replica()
        .context("failed to build read replica client")?;

    println!(
        "Starting benchmark for {}",
        humantime::format_duration(cli.duration)
    );

    let duration = cli.duration;
    thread::spawn(move || simulate_reader_connection(reader, secondary, duration, tx));

    let mut measurements = Measurements::new(cli.duration);
    let pb = ProgressBar::new(cli.duration.as_secs());
    pb.set_style(
        ProgressStyle::default_bar()
            .template("{wide_bar} {pos}/{len} [{elapsed_precise}] ETA: {eta_precise}")
            .unwrap()
            .progress_chars("#>-"),
    );

    let start = Instant::now();
    let mut offset = Duration::from_secs(0);
    loop {
        match rx.recv_timeout(Duration::from_secs(1)) {
            Ok(duration) => measurements.push(duration),
            Err(RecvTimeoutError::Disconnected) => break,
            _ => {}
        }

        let elapsed = start.elapsed();
        let difference = elapsed.saturating_sub(offset).as_secs();
        if difference > 0 {
            pb.inc(elapsed.saturating_sub(offset).as_secs());
            offset = elapsed
        }
    }

    pb.finish_with_message("Done");
    Ok(measurements)
}
