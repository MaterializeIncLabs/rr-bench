use crate::task_handle::TaskCompletion;
use crate::{WriteOperation, PrimaryDatabase};
use anyhow::{Context, Result};
use fake::faker::address::raw::StreetName;
use fake::faker::company::raw::{CompanyName, Industry};
use fake::faker::name::raw::Name;
use fake::locales::EN;
use fake::Fake;
use rand::distributions::Alphanumeric;
use rand::prelude::SliceRandom;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use std::thread::sleep;
use std::time::Duration;

const INSERT_PERCENTAGE: u32 = 45;
const UPDATE_PERCENTAGE: u32 = 45;

pub struct PrimarySimulator<DB: PrimaryDatabase> {
    db: DB,
    tps: u32,
    rng: StdRng,
    completion_tracker: TaskCompletion,
}

impl<DB: PrimaryDatabase> PrimarySimulator<DB> {
    pub fn new(db: DB, tps: u32, seed: u64, completion_tracker: TaskCompletion) -> Self {
        let rng = StdRng::seed_from_u64(seed);
        PrimarySimulator {
            db,
            tps,
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
            0 => WriteOperation::InsertCustomer {
                name: Name(EN).fake_with_rng(&mut self.rng),
                address: StreetName(EN).fake_with_rng(&mut self.rng),
            },
            1 => {
                let customer_id = self.db.get_random_customer_id()?;

                WriteOperation::InsertAccount {
                    customer_id,
                    account_type: ["Savings", "Checking", "Brokerage", "Investment"]
                        .choose(&mut self.rng)
                        .unwrap()
                        .to_string(),
                    balance: self.rng.gen_range(0.0..10000.0),
                    parent_account_id: None,
                }
            }
            2 => WriteOperation::InsertSecurity {
                ticker: ticker(&mut self.rng),
                name: CompanyName(EN).fake_with_rng(&mut self.rng),
                sector: Industry(EN).fake_with_rng(&mut self.rng),
            },
            3 => {
                let account_id = self.db.get_random_account_id()?;
                let security_id = self.db.get_random_security_id()?;

                WriteOperation::InsertTrade {
                    account_id,
                    security_id,
                    trade_type: ["buy", "sell"].choose(&mut self.rng).unwrap().to_string(),
                    quantity: self.rng.gen_range(1..1000),
                    price: self.rng.gen_range(100.0..500.0),
                    parent_trade_id: None,
                }
            }
            4 => {
                let account_id = self.db.get_random_account_id()?;
                let security_id = self.db.get_random_security_id()?;

                WriteOperation::InsertOrder {
                    account_id,
                    security_id,
                    order_type: ["buy", "sell"].choose(&mut self.rng).unwrap().to_string(),
                    quantity: self.rng.gen_range(1..1000),
                    limit_price: self.rng.gen_range(1..1000) as f64,
                    status: ["pending", "completed", "canceled"]
                        .choose(&mut self.rng)
                        .unwrap()
                        .to_string(),
                    parent_order_id: None,
                }
            }
            _ => {
                let security_id = self.db.get_random_security_id()?;

                WriteOperation::InsertMarketData {
                    security_id,
                    price: self.rng.gen_range(100.0..500.0),
                    volume: self.rng.gen_range(1000..100000),
                }
            }
        };

        Ok(operation)
    }

    fn generate_update(&mut self) -> Result<WriteOperation> {
        let operation = match self.rng.gen_range(0..5) {
            0 => {
                let customer_id = self.db.get_random_customer_id()?;
                WriteOperation::UpdateCustomer {
                    customer_id,
                    address: StreetName(EN).fake_with_rng(&mut self.rng),
                }
            }
            1 => {
                let account_id = self.db.get_random_account_id()?;
                WriteOperation::UpdateAccount {
                    account_id,
                    balance: self.rng.gen_range(0.0..10000.0),
                }
            }
            2 => {
                let trade_id = self.db.get_random_trade_id()?;
                WriteOperation::UpdateTrade {
                    trade_id,
                    price: self.rng.gen_range(100.0..500.0),
                }
            }
            3 => {
                let order_id = self.db.get_random_order_id()?;
                WriteOperation::UpdateOrder {
                    order_id,
                    status: ["pending", "completed", "canceled"]
                        .choose(&mut self.rng)
                        .unwrap()
                        .to_string(),
                    limit_price: self.rng.gen_range(100.0..500.0),
                }
            }
            _ => {
                let market_data_id = self.db.get_random_market_data_id()?;
                WriteOperation::UpdateMarketData {
                    market_data_id,
                    price: self.rng.gen_range(100.0..500.0),
                    volume: self.rng.gen_range(1000..100000) as f64,
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

fn ticker<R: Rng + ?Sized>(rng: &mut R) -> String {
    (0..4).map(|_| rng.sample(Alphanumeric) as char).collect()
}
