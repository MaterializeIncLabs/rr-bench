use crate::operations::ReadOperation;
use crate::task_handle::TaskHandle;
use crate::{PrimaryDatabase, ReadReplica};
use anyhow::Result;
use indicatif::ProgressBar;
use std::sync::mpsc::Sender;
use std::time::{Duration, Instant};
use strum::IntoEnumIterator;

/// `ReaderSimulator` runs a series of read operations against a `ReadReplica`.
///
/// Timing is based on the cumulative duration of the read operations (experiment duration),
/// rather than real-world time. This ensures that the simulation focuses on measuring the
/// performance of the read replica itself, without including time spent on other tasks,
/// such as querying the primary database.
pub struct ReaderSimulator<R: ReadReplica, P: PrimaryDatabase> {
    reader: InstrumentedReader<R>,
    primary: P,
    duration: Duration,
    timings: Sender<Duration>,
    pb: ExperimentProgressBar,
    /// This handle is used solely for its `Drop` implementation, which triggers cleanup
    /// or signaling when the `ReaderSimulator` is completed.
    _handle: TaskHandle,
}

impl<R: ReadReplica, P: PrimaryDatabase> ReaderSimulator<R, P> {
    pub fn new(
        reader: R,
        primary: P,
        duration: Duration,
        timings: Sender<Duration>,
        pb: ProgressBar,
        handle: TaskHandle,
    ) -> Self {
        Self {
            reader: InstrumentedReader::new(reader),
            primary,
            duration,
            timings,
            pb: ExperimentProgressBar::new(pb),
            _handle: handle,
        }
    }

    pub fn run(&mut self) -> Result<()> {
        let mut iter = ReadOperation::iter().cycle();

        while self.reader.experiment_duration < self.duration {
            let measurement = match iter.next().unwrap() {
                ReadOperation::CustomerPortfolio => {
                    let customer_id = self.primary.get_random_customer_id()?;
                    self.reader.customer_portfolio(customer_id)?
                }
                ReadOperation::TopPerformers => self.reader.top_performers()?,
                ReadOperation::MarketOverview => {
                    let sector = self.primary.get_random_sector()?;
                    self.reader.market_overview(&sector)?
                }
                ReadOperation::RecentLargeTrades => {
                    let account_id = self.primary.get_random_account_id()?;
                    self.reader.recent_large_trades(account_id)?
                }
                ReadOperation::CustomerOrderBook => {
                    let customer_id = self.primary.get_random_customer_id()?;
                    self.reader.customer_order_book(customer_id)?
                }
                ReadOperation::SectorPerformance => {
                    let sector = self.primary.get_random_sector()?;
                    self.reader.sector_performance(sector)?
                }
                ReadOperation::AccountActivitySummary => {
                    let account_id = self.primary.get_random_account_id()?;
                    self.reader.account_activity_summary(account_id)?
                }
                ReadOperation::DailyMarketMovements => {
                    let security_id = self.primary.get_random_security_id()?;
                    self.reader.daily_market_movements(security_id)?
                }
                ReadOperation::HighValueCustomers => self.reader.high_value_customers()?,
                ReadOperation::PendingOrdersSummary => {
                    let ticker = self.primary.get_random_ticker()?;
                    self.reader.pending_orders_summary(&ticker)?
                }
                ReadOperation::TradeVolumeByHour => self.reader.trade_volume_by_hour()?,
                ReadOperation::TopSecuritiesBySector => {
                    let sector = self.primary.get_random_sector()?;
                    self.reader.top_securities_by_sector(sector)?
                }
                ReadOperation::RecentTradesByAccount => {
                    let account_id = self.primary.get_random_account_id()?;
                    self.reader.recent_trades_by_account(account_id)?
                }
                ReadOperation::OrderFulfillmentRates => {
                    let customer_id = self.primary.get_random_customer_id()?;
                    self.reader.order_fulfillment_rates(customer_id)?
                }
                ReadOperation::SectorOrderActivity => {
                    let sector = self.primary.get_random_sector()?;
                    self.reader.sector_order_activity(sector)?
                }
            };

            self.pb.inc(measurement);
            if self.timings.send(measurement).is_err() {
                break;
            }
        }

        self.pb.finish();
        Ok(())
    }
}

struct ExperimentProgressBar {
    pb: ProgressBar,
    offset: Duration,
}

impl ExperimentProgressBar {
    fn new(pb: ProgressBar) -> Self {
        Self {
            pb,
            offset: Duration::from_secs(0),
        }
    }

    fn inc(&mut self, duration: Duration) {
        self.offset += duration;
        if self.offset.as_secs() > 0 {
            self.pb.inc(self.offset.as_secs());
            self.offset = Duration::from_secs(0);
        }
    }

    fn finish(&self) {
        self.pb.finish()
    }
}

/// `InstrumentedReader` wraps a `ReadReplica` and times individual read operations.
/// It’s used in benchmarking to accurately measure how long each operation takes,
/// without including time spent on other tasks like querying the primary database.
struct InstrumentedReader<R> {
    handle: R,
    experiment_duration: Duration,
}

impl<R> InstrumentedReader<R> {
    fn new(reader: R) -> Self {
        Self {
            handle: reader,
            experiment_duration: Duration::from_secs(0),
        }
    }
}

impl<R: ReadReplica> InstrumentedReader<R> {
    fn customer_portfolio(&mut self, customer_id: i32) -> Result<Duration> {
        let start = Instant::now();
        self.handle.customer_portfolio(customer_id)?;
        let duration = start.elapsed();
        self.experiment_duration += duration;
        Ok(duration)
    }

    fn top_performers(&mut self) -> Result<Duration> {
        let start = Instant::now();
        self.handle.top_performers()?;
        let duration = start.elapsed();
        self.experiment_duration += duration;
        Ok(duration)
    }

    fn market_overview(&mut self, sector: &str) -> Result<Duration> {
        let start = Instant::now();
        self.handle.market_overview(sector)?;
        let duration = start.elapsed();
        self.experiment_duration += duration;
        Ok(duration)
    }

    fn recent_large_trades(&mut self, account_id: i32) -> Result<Duration> {
        let start = Instant::now();
        self.handle.recent_large_trades(account_id)?;
        let duration = start.elapsed();
        self.experiment_duration += duration;
        Ok(duration)
    }

    fn customer_order_book(&mut self, customer_id: i32) -> Result<Duration> {
        let start = Instant::now();
        self.handle.customer_order_book(customer_id)?;
        let duration = start.elapsed();
        self.experiment_duration += duration;
        Ok(duration)
    }

    fn sector_performance(&mut self, sector: String) -> Result<Duration> {
        let start = Instant::now();
        self.handle.sector_performance(sector)?;
        let duration = start.elapsed();
        self.experiment_duration += duration;
        Ok(duration)
    }

    fn account_activity_summary(&mut self, account_id: i32) -> Result<Duration> {
        let start = Instant::now();
        self.handle.account_activity_summary(account_id)?;
        let duration = start.elapsed();
        self.experiment_duration += duration;
        Ok(duration)
    }

    fn daily_market_movements(&mut self, security_id: i32) -> Result<Duration> {
        let start = Instant::now();
        self.handle.daily_market_movements(security_id)?;
        let duration = start.elapsed();
        self.experiment_duration += duration;
        Ok(duration)
    }

    fn high_value_customers(&mut self) -> Result<Duration> {
        let start = Instant::now();
        self.handle.high_value_customers()?;
        let duration = start.elapsed();
        self.experiment_duration += duration;
        Ok(duration)
    }

    fn pending_orders_summary(&mut self, ticker: &str) -> Result<Duration> {
        let start = Instant::now();
        self.handle.pending_orders_summary(ticker)?;
        let duration = start.elapsed();
        self.experiment_duration += duration;
        Ok(duration)
    }

    fn trade_volume_by_hour(&mut self) -> Result<Duration> {
        let start = Instant::now();
        self.handle.trade_volume_by_hour()?;
        let duration = start.elapsed();
        self.experiment_duration += duration;
        Ok(duration)
    }

    fn top_securities_by_sector(&mut self, sector: String) -> Result<Duration> {
        let start = Instant::now();
        self.handle.top_securities_by_sector(sector)?;
        let duration = start.elapsed();
        self.experiment_duration += duration;
        Ok(duration)
    }

    fn recent_trades_by_account(&mut self, account_id: i32) -> Result<Duration> {
        let start = Instant::now();
        self.handle.recent_trades_by_account(account_id)?;
        let duration = start.elapsed();
        self.experiment_duration += duration;
        Ok(duration)
    }

    fn order_fulfillment_rates(&mut self, customer_id: i32) -> Result<Duration> {
        let start = Instant::now();
        self.handle.order_fulfillment_rates(customer_id)?;
        let duration = start.elapsed();
        self.experiment_duration += duration;
        Ok(duration)
    }

    fn sector_order_activity(&mut self, sector: String) -> Result<Duration> {
        let start = Instant::now();
        self.handle.sector_order_activity(sector)?;
        let duration = start.elapsed();
        self.experiment_duration += duration;
        Ok(duration)
    }
}
