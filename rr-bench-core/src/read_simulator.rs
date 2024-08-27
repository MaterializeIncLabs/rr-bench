use crate::task_handle::TaskHandle;
use crate::{PrimaryDatabase, ReadReplica};
use anyhow::Result;
use indicatif::ProgressBar;
use std::sync::mpsc::Sender;
use std::time::{Duration, Instant};

pub struct ReaderSimulator<R: ReadReplica, P: PrimaryDatabase> {
    reader: R,
    primary: P,
    duration: Duration,
    timings: Sender<Duration>,
    pb: ProgressBar,
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
            reader,
            primary,
            duration,
            timings,
            pb,
            _handle: handle,
        }
    }

    pub fn run(&mut self) -> Result<()> {
        let mut iter = (0..=14).cycle();

        let mut offset = Duration::from_secs(0);
        let mut experiment_duration = Duration::from_secs(0);
        while experiment_duration < self.duration {
            let id = iter.next().unwrap();
            let measurement = match id {
                0 => {
                    let customer_id = self.primary.get_random_customer_id()?;
                    let instant = Instant::now();
                    self.reader.customer_portfolio(customer_id)?;
                    instant.elapsed()
                }
                1 => {
                    let instant = Instant::now();
                    self.reader.top_performers()?;
                    instant.elapsed()
                }
                2 => {
                    let sector = self.primary.get_random_sector()?;
                    let instant = Instant::now();
                    self.reader.market_overview(&sector)?;
                    instant.elapsed()
                }
                3 => {
                    let account_id = self.primary.get_random_account_id()?;
                    let instant = Instant::now();
                    self.reader.recent_large_trades(account_id)?;
                    instant.elapsed()
                }
                4 => {
                    let customer_id = self.primary.get_random_customer_id()?;
                    let instant = Instant::now();
                    self.reader.customer_portfolio(customer_id)?;
                    instant.elapsed()
                }
                5 => {
                    let sector = self.primary.get_random_sector()?;
                    let instant = Instant::now();
                    self.reader.sector_performance(sector)?;
                    instant.elapsed()
                }
                6 => {
                    let account_id = self.primary.get_random_account_id()?;
                    let instant = Instant::now();
                    self.reader.account_activity_summary(account_id)?;
                    instant.elapsed()
                }
                7 => {
                    let security_id = self.primary.get_random_security_id()?;
                    let instant = Instant::now();
                    self.reader.daily_market_movements(security_id)?;
                    instant.elapsed()
                }
                8 => {
                    let instant = Instant::now();
                    self.reader.high_value_customers()?;
                    instant.elapsed()
                }
                9 => {
                    let ticker = self.primary.get_random_ticker()?;
                    let instant = Instant::now();
                    self.reader.pending_orders_summary(&ticker)?;
                    instant.elapsed()
                }
                10 => {
                    let instant = Instant::now();
                    self.reader.trade_volume_by_hour()?;
                    instant.elapsed()
                }
                11 => {
                    let sector = self.primary.get_random_sector()?;
                    let instant = Instant::now();
                    self.reader.top_securities_by_sector(sector)?;
                    instant.elapsed()
                }
                12 => {
                    let account_id = self.primary.get_random_account_id()?;
                    let instant = Instant::now();
                    self.reader.recent_trades_by_account(account_id)?;
                    instant.elapsed()
                }
                13 => {
                    let customer_id = self.primary.get_random_customer_id()?;
                    let instant = Instant::now();
                    self.reader.order_fulfillment_rates(customer_id)?;
                    instant.elapsed()
                }
                14 => {
                    let sector = self.primary.get_random_sector()?;
                    let instant = Instant::now();
                    self.reader.sector_order_activity(sector)?;
                    instant.elapsed()
                }
                15 => {
                    let instant = Instant::now();
                    self.reader.cascading_order_cancellation_alert()?;
                    instant.elapsed()
                }
                x => panic!("View {x} does not exist"),
            };

            experiment_duration += measurement;
            offset += measurement;

            if offset.as_secs() >= 1 {
                self.pb.inc(offset.as_secs());
                offset = Duration::from_secs(0);
            }

            if self.timings.send(measurement).is_err() {
                break;
            }
        }

        self.pb.finish();
        Ok(())
    }
}
