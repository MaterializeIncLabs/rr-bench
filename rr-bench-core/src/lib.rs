#![allow(clippy::needless_doctest_main)]

use crate::config::{Args, Cli};
use crate::measurements::Measurements;
use crate::operations::WriteOperation;
use crate::primary_simulator::PrimarySimulator;
use crate::read_simulator::ReaderSimulator;
use crate::task_handle::new_task_handles;
use anyhow::{Context, Result};
use clap::{Arg, ArgMatches};
use indicatif::{MultiProgress, ProgressBar, ProgressStyle};
use std::process::exit;
use std::sync::mpsc;
use std::sync::mpsc::RecvTimeoutError;
use std::thread;
use std::time::Duration;

pub use clap;

mod config;
mod measurements;
pub mod operations;
mod pretty_duration;
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

    fn get_random_ticker(&mut self) -> Result<String>;

    fn get_random_sector(&mut self) -> Result<String>;

    fn execute_command(&self, op: WriteOperation) -> Result<()>;
}

/// The `ReadReplica` trait defines the interface for interacting with a read replica
/// in a benchmarking environment. This trait includes methods for executing various
/// read operations that are typical in OLTP systems, such as fetching customer portfolios
/// or querying market data.
pub trait ReadReplica: Send {
    fn customer_portfolio(&mut self, customer_id: i32) -> Result<()>;

    fn top_performers(&mut self) -> Result<()>;

    fn market_overview(&mut self, sector: &str) -> Result<()>;

    fn recent_large_trades(&mut self, account_id: i32) -> Result<()>;

    fn customer_order_book(&mut self, customer_id: i32) -> Result<()>;

    fn sector_performance(&mut self, sector: String) -> Result<()>;

    fn account_activity_summary(&mut self, account_id: i32) -> Result<()>;

    fn daily_market_movements(&mut self, security_id: i32) -> Result<()>;

    fn high_value_customers(&mut self) -> Result<()>;

    fn pending_orders_summary(&mut self, ticker: &str) -> Result<()>;

    fn trade_volume_by_hour(&mut self) -> Result<()>;

    fn top_securities_by_sector(&mut self, sector: String) -> Result<()>;

    fn recent_trades_by_account(&mut self, account_id: i32) -> Result<()>;

    fn order_fulfillment_rates(&mut self, customer_id: i32) -> Result<()>;

    fn sector_order_activity(&mut self, sector: String) -> Result<()>;

    fn cascading_order_cancellation_alert(&mut self) -> Result<()>;
}

/// The `benchmark` function runs a benchmarking test using the provided closures to set up
/// the benchmarking environment and create a `Benchmark` instance.
///
/// This function sets up the benchmarking environment, spawns threads to simulate primary
/// database writes and read replica queries, and collects measurements over the specified duration.
///
/// # Arguments
///
/// * `args` - A closure that returns an iterator of arguments, each implementing `Into<Arg>`.
///            These arguments are for configuring the command-line interface using `clap`.
///            Implementors can use this to add their own command-line arguments for benchmark-specific
///            configurations.
/// * `f` - A closure that takes an `ArgMatches` (parsed command-line arguments) and returns a
///         `Result` containing an instance of a type that implements the `Benchmark` trait.
///
/// # Example
///
/// ```no_compile
/// use clap::{Arg, Command, ArgMatches};
/// use std::error::Error;
///
/// struct MyBenchmark;
///
/// impl<'a> Benchmark<'a> for MyBenchmark {
///     // Implement required methods...
/// }
///
/// fn main() -> Result<(), Box<dyn Error>> {
///     benchmark(
///         || vec![Arg::new("config").long("config").takes_value(true).about("Sets the configuration file")],
///         |matches: ArgMatches| {
///             // Use matches to configure the benchmark
///             Ok(MyBenchmark)
///         },
///     );
///     Ok(())
/// }
/// ```
pub fn benchmark<C, I, A, B, F>(args: C, f: F)
where
    C: Fn() -> I,
    I: IntoIterator<Item = A>,
    A: Into<Arg>,
    B: for<'a> Benchmark<'a>,
    F: Fn(ArgMatches) -> Result<B>,
{
    let args = Args::new(args());
    let cli = args.parse();

    match inner(cli, f) {
        Ok(measurements) => println!("{}", measurements),
        Err(e) => {
            eprintln!("{:?}", e);
            exit(1)
        }
    }
}

fn inner<B: for<'a> Benchmark<'a>, F>(cli: Cli, f: F) -> Result<Measurements>
where
    F: Fn(ArgMatches) -> Result<B>,
{
    let benchmark: B = f(cli.matches)?;
    let (handle, tracker) = new_task_handles();

    thread::scope(|s| {
        let primary = benchmark
            .primary_database()
            .context("failed to build primary database client")?;

        s.spawn(move || {
            eprintln!("starting primary database simulator");
            let mut simulator =
                PrimarySimulator::new(primary, cli.transactions_per_second, 42, tracker);
            if let Err(e) = simulator.run() {
                eprintln!("{:?}", e);
                exit(1)
            }
            eprintln!("shutting down primary database simulator");
        });

        let (tx, rx) = mpsc::channel();

        println!(
            "Starting benchmark for {}",
            humantime::format_duration(cli.duration)
        );

        let m = MultiProgress::new();
        let style = ProgressStyle::default_bar()
            .template("{msg} {wide_bar} {pos}/{len} [{elapsed_precise}] ETA: {eta_precise}")
            .unwrap()
            .progress_chars("#>-");

        println!("Spawning {} clients", cli.concurrency);
        for i in 0..cli.concurrency {
            let secondary = benchmark
                .primary_database()
                .context("failed to build primary database client")?;

            let reader = benchmark
                .read_replica()
                .context("failed to build read replica client")?;

            let tx = tx.clone();
            let handle = handle.clone();
            let duration = cli.duration;

            let pb = m.add(ProgressBar::new(duration.as_secs()));
            pb.set_style(style.clone());
            pb.set_message(format!("client {i}"));

            s.spawn(move || {
                let mut simulator =
                    ReaderSimulator::new(reader, secondary, duration, tx, pb, handle);
                if let Err(e) = simulator.run() {
                    eprintln!("{:?}", e);
                    exit(1)
                };
            });
        }

        drop(tx);
        drop(handle);

        let mut measurements = Measurements::new(cli.duration);
        loop {
            match rx.recv_timeout(Duration::from_secs(1)) {
                Ok(duration) => measurements.push(duration),
                Err(RecvTimeoutError::Disconnected) => break,
                _ => {}
            }
        }
        Ok(measurements)
    })
}
