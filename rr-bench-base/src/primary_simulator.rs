use crate::task_handle::TaskCompletion;
use crate::{PrimaryDatabase, WriteOperation};
use anyhow::{Context, Result};
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use rr_bench_core::DataGenerator;
use std::thread::sleep;
use std::time::Duration;

const INSERT_PERCENTAGE: u32 = 45;
const UPDATE_PERCENTAGE: u32 = 45;

pub struct PrimarySimulator<DB: PrimaryDatabase> {
    db: DB,
    tps: u32,
    rng: StdRng,
    gen: DataGenerator,
    completion_tracker: TaskCompletion,
}

impl<DB: PrimaryDatabase> PrimarySimulator<DB> {
    pub fn new(db: DB, tps: u32, seed: u64, completion_tracker: TaskCompletion) -> Self {
        let rng = StdRng::seed_from_u64(seed);
        let gen = DataGenerator::new(seed);
        PrimarySimulator {
            db,
            tps,
            gen,
            rng,
            completion_tracker,
        }
    }

    pub fn run(&mut self) -> Result<()> {
        let interval = Duration::from_secs(1) / self.tps;
        while !self.completion_tracker.is_done() {
            let op = self.generate_operations()?;
            if let Err(e) = self.db.execute_command(op) {
                return Err(e).context("failed to execute command");
            }
            sleep(interval);
        }

        Ok(())
    }

    fn generate_operations(&mut self) -> Result<WriteOperation> {
        let op_type = self.rng.gen_range(0..100);
        if op_type < INSERT_PERCENTAGE {
            self.generate_insert()
        } else if op_type < UPDATE_PERCENTAGE {
            self.generate_update()
        } else {
            self.generate_delete()
        }
    }

    fn generate_insert(&mut self) -> Result<WriteOperation> {
        let operation = match self.rng.gen_range(0..6) {
            0 => {
                let customer = self.gen.generate_customer();
                WriteOperation::InsertCustomer {
                    name: customer.name,
                    address: customer.address,
                }
            }
            1 => {
                let customer_id = self.db.get_random_customer_id()?;
                let account = self.gen.generate_account();
                WriteOperation::InsertAccount {
                    customer_id,
                    account_type: account.account_type,
                    balance: account.balance,
                    parent_account_id: None,
                }
            }
            2 => {
                let security = self.gen.generate_security();
                WriteOperation::InsertSecurity {
                    ticker: security.ticker,
                    name: security.name,
                    sector: security.sector,
                }
            }
            3 => {
                let account_id = self.db.get_random_account_id()?;
                let security_id = self.db.get_random_security_id()?;

                let trade = self.gen.generate_trade();

                WriteOperation::InsertTrade {
                    account_id,
                    security_id,
                    trade_type: trade.trade_type,
                    quantity: trade.quantity,
                    price: trade.price,
                    parent_trade_id: None,
                }
            }
            4 => {
                let account_id = self.db.get_random_account_id()?;
                let security_id = self.db.get_random_security_id()?;

                let order = self.gen.generate_order();
                WriteOperation::InsertOrder {
                    account_id,
                    security_id,
                    order_type: order.order_type,
                    quantity: order.quantity,
                    limit_price: order.limit_price,
                    status: order.status,
                    parent_order_id: None,
                }
            }
            _ => {
                let security_id = self.db.get_random_security_id()?;
                let market_data = self.gen.generate_market_data();
                WriteOperation::InsertMarketData {
                    security_id,
                    price: market_data.price,
                    volume: market_data.volume,
                }
            }
        };

        Ok(operation)
    }

    fn generate_update(&mut self) -> Result<WriteOperation> {
        let operation = match self.rng.gen_range(0..5) {
            0 => {
                let customer_id = self.db.get_random_customer_id()?;
                let customer = self.gen.generate_customer();
                WriteOperation::UpdateCustomer {
                    customer_id,
                    address: customer.address,
                }
            }
            1 => {
                let account_id = self.db.get_random_account_id()?;
                let account = self.gen.generate_account();
                WriteOperation::UpdateAccount {
                    account_id,
                    balance: account.balance,
                }
            }
            2 => {
                let trade_id = self.db.get_random_trade_id()?;
                let trade = self.gen.generate_trade();
                WriteOperation::UpdateTrade {
                    trade_id,
                    price: trade.price,
                }
            }
            3 => {
                let order_id = self.db.get_random_order_id()?;
                let order = self.gen.generate_order();
                WriteOperation::UpdateOrder {
                    order_id,
                    status: order.status,
                    limit_price: order.limit_price,
                }
            }
            _ => {
                let market_data_id = self.db.get_random_market_data_id()?;
                let market_data = self.gen.generate_market_data();
                WriteOperation::UpdateMarketData {
                    market_data_id,
                    price: market_data.price,
                    volume: market_data.volume,
                }
            }
        };

        Ok(operation)
    }

    fn generate_delete(&mut self) -> Result<WriteOperation> {
        let operation = match self.rng.gen_range(0..6) {
            0 => WriteOperation::DeleteCustomer {
                customer_id: self.db.get_random_customer_id()?,
            },
            1 => WriteOperation::DeleteAccount {
                account_id: self.db.get_random_account_id()?,
            },
            2 => WriteOperation::DeleteSecurity {
                security_id: self.db.get_random_security_id()?,
            },
            3 => WriteOperation::DeleteTrade {
                trade_id: self.db.get_random_trade_id()?,
            },
            4 => WriteOperation::DeleteOrder {
                order_id: self.db.get_random_order_id()?,
            },
            _ => WriteOperation::DeleteMarketData {
                market_data_id: self.db.get_random_market_data_id()?,
            },
        };

        Ok(operation)
    }
}
