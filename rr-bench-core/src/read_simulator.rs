use crate::{PrimaryDatabase, ReadReplica};
use anyhow::{Context, Result};
use std::sync::mpsc::Sender;
use std::time::{Duration, Instant};

pub fn simulate_reader_connection<R: ReadReplica, P: PrimaryDatabase>(
    reader: R,
    primary: P,
    duration: Duration,
    timings: Sender<Duration>,
) -> Result<()> {
    let start = Instant::now();
    let mut iter = (0..=15).cycle();

    while start.elapsed() < duration {
        let measurement = match iter.next().unwrap() {
            0 => {
                let customer_id = primary.get_random_customer_id()?;
                let instant = Instant::now();
                reader.customer_portfolio(customer_id)?;
                instant.elapsed()
            }
            1 => {
                let instant = Instant::now();
                reader.top_performers()?;
                instant.elapsed()
            }
            2 => {
                let instant = Instant::now();
                reader.market_overview()?;
                instant.elapsed()
            }
            3 => {
                let instant = Instant::now();
                reader.recent_large_trades()?;
                instant.elapsed()
            }
            4 => {
                let customer_id = primary.get_random_customer_id()?;
                let instant = Instant::now();
                reader.customer_portfolio(customer_id)?;
                instant.elapsed()
            }
            5 => {
                let sector = primary.get_random_sector()?;
                let instant = Instant::now();
                reader.sector_performance(sector)?;
                instant.elapsed()
            }
            6 => {
                let account_id = primary.get_random_account_id()?;
                let instant = Instant::now();
                reader.account_activity_summary(account_id)?;
                instant.elapsed()
            }
            7 => {
                let security_id = primary.get_random_security_id()?;
                let instant = Instant::now();
                reader.daily_market_movements(security_id)?;
                instant.elapsed()
            }
            8 => {
                let instant = Instant::now();
                reader.high_value_customers()?;
                instant.elapsed()
            }
            9 => {
                let instant = Instant::now();
                reader.pending_orders_summary()?;
                instant.elapsed()
            }
            10 => {
                let instant = Instant::now();
                reader.trade_volume_by_hour()?;
                instant.elapsed()
            }
            11 => {
                let sector = primary.get_random_sector()?;
                let instant = Instant::now();
                reader.top_securities_by_sector(sector)?;
                instant.elapsed()
            }
            12 => {
                let account_id = primary.get_random_account_id()?;
                let instant = Instant::now();
                reader.recent_trades_by_account(account_id)?;
                instant.elapsed()
            }
            13 => {
                let customer_id = primary.get_random_customer_id()?;
                let instant = Instant::now();
                reader.order_fulfillment_rates(customer_id)?;
                instant.elapsed()
            }
            14 => {
                let sector = primary.get_random_sector()?;
                let instant = Instant::now();
                reader.sector_order_activity(sector)?;
                instant.elapsed()
            }
            15 => {
                let instant = Instant::now();
                reader.cascading_order_cancellation_alert()?;
                instant.elapsed()
            }
            x => panic!("View {x} does not exist"),
        };

        timings.send(measurement).context("failed to send timing")?;
    }

    Ok(())
}
