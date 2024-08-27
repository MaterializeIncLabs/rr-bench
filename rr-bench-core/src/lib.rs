#![allow(clippy::needless_doctest_main)]

use crate::config::Cli;
pub use crate::config::Config;
use crate::measurements::Measurements;
use crate::operations::Operation;
use crate::primary_simulator::PrimarySimulator;
use crate::read_simulator::simulate_reader_connection;
use crate::task_handle::new_task_handles;
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
pub mod operations;
mod primary_simulator;
mod read_simulator;
mod task_handle;

/// The `Benchmark` trait defines the interface for setting up a database benchmarking environment.
/// Implementors of this trait are responsible for providing access to both the primary database
/// and the read replica.
pub trait Benchmark<'a>: Send {
    type Writer: PrimaryDatabase + 'a;

    type Reader: ReadReplica;

    /// Provides access to the primary database. This
    /// method may be called multiple times but may utilize
    /// connection pooling.
    fn primary_database(&'a self) -> Result<Self::Writer>;

    /// Provides access to the read replica. This
    /// method may be called multiple times and should
    /// return a handle with a new connection each time.
    fn read_replica(&self) -> Result<Self::Reader>;
}

/// The `PrimaryDatabase` trait defines the interface for interacting with the primary database
/// in a benchmarking environment. This trait includes methods for retrieving random IDs from
/// various tables and executing operations such as inserts, updates, or deletes.
pub trait PrimaryDatabase: Send {
    fn get_random_customer_id(&mut self) -> Result<i32>;

    fn get_random_account_id(&mut self) -> Result<i32>;

    fn get_random_security_id(&mut self) -> Result<i32>;

    fn get_random_trade_id(&mut self) -> Result<i32>;

    fn get_random_order_id(&mut self) -> Result<i32>;

    fn get_random_market_data_id(&mut self) -> Result<i32>;

    fn get_random_sector(&mut self) -> Result<String>;

    fn execute_command(&self, op: Operation) -> Result<()>;
}

/// The `ReadReplica` trait defines the interface for interacting with a read replica
/// in a benchmarking environment. This trait includes methods for executing various
/// read operations that are typical in OLTP systems, such as fetching customer portfolios
/// or querying market data.
pub trait ReadReplica: Send {
    fn customer_portfolio(&mut self, customer_id: i32) -> Result<()>;

    fn top_performers(&mut self) -> Result<()>;

    fn market_overview(&mut self) -> Result<()>;

    fn recent_large_trades(&mut self) -> Result<()>;

    fn customer_order_book(&mut self, customer_id: i32) -> Result<()>;

    fn sector_performance(&mut self, sector: String) -> Result<()>;

    fn account_activity_summary(&mut self, account_id: i32) -> Result<()>;

    fn daily_market_movements(&mut self, security_id: i32) -> Result<()>;

    fn high_value_customers(&mut self) -> Result<()>;

    fn pending_orders_summary(&mut self) -> Result<()>;

    fn trade_volume_by_hour(&mut self) -> Result<()>;

    fn top_securities_by_sector(&mut self, sector: String) -> Result<()>;

    fn recent_trades_by_account(&mut self, account_id: i32) -> Result<()>;

    fn order_fulfillment_rates(&mut self, customer_id: i32) -> Result<()>;

    fn sector_order_activity(&mut self, sector: String) -> Result<()>;

    fn cascading_order_cancellation_alert(&mut self) -> Result<()>;
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
/// ```compile_fail
/// fn main() {
///     benchmark(|config| {
///         MyBenchmark::new(config)
///     });
/// }
/// ```
pub fn benchmark<B: for<'a> Benchmark<'a>, F>(f: F)
where
    F: Fn(Config) -> Result<B>,
{
    match inner(f) {
        Ok(measurements) => println!("{}", measurements),
        Err(e) => {
            eprintln!("{:?}", e);
            exit(1)
        }
    }
}

fn inner<B: for<'a> Benchmark<'a>, F>(f: F) -> Result<Measurements>
where
    F: Fn(Config) -> Result<B>,
{
    let cli: Cli = Cli::parse();
    let benchmark: B = f(cli.get_config())?;

    let (handle, tracker) = new_task_handles();

    thread::scope(|s| {
        let primary = benchmark
            .primary_database()
            .context("failed to build primary database client")?;

        s.spawn(move || {
            let mut simulator =
                PrimarySimulator::new(primary, cli.transactions_per_second, 42, tracker);
            if let Err(e) = simulator.run() {
                eprintln!("{:?}", e);
                exit(1)
            }
        });

        let (tx, rx) = mpsc::channel();

        println!(
            "Starting benchmark for {}",
            humantime::format_duration(cli.duration)
        );

        let duration = cli.duration;

        println!("Spawning {} clients", cli.concurrency);
        for _ in 0..cli.concurrency {
            let secondary = benchmark
                .primary_database()
                .context("failed to build primary database client")?;

            let reader = benchmark
                .read_replica()
                .context("failed to build read replica client")?;

            let tx = tx.clone();
            let handle = handle.clone();
            s.spawn(move || simulate_reader_connection(reader, secondary, duration, tx, handle));
        }

        drop(tx);
        drop(handle);

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
        while start.elapsed() < cli.duration {
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
    })
}
