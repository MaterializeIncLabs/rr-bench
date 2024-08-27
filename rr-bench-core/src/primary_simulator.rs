use crate::{Operation, PrimaryDatabase};
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

pub struct PrimarySimulator<DB: PrimaryDatabase> {
    db: DB,
    tps: u32,
    rng: StdRng,
}

impl<DB: PrimaryDatabase> PrimarySimulator<DB> {
    pub fn new(db: DB, tps: u32, seed: u64) -> Self {
        let rng = StdRng::seed_from_u64(seed);
        PrimarySimulator { db, tps, rng }
    }

    pub fn run(&mut self) -> Result<()> {
        let interval = Duration::from_secs(1) / self.tps;
        loop {
            let op = self.generate_operations()?;
            if let Err(e) = self.db.execute_command(op) {
                return Err(e).context("failed to execute command");
            }
            sleep(interval);
        }
    }

    fn generate_operations(&mut self) -> Result<Operation> {
        let op_type = self.rng.gen_range(0..100);
        if op_type < 45 {
            self.generate_insert()
        } else if op_type < 90 {
            self.generate_update()
        } else {
            self.generate_delete()
        }
    }

    fn generate_insert(&mut self) -> Result<Operation> {
        let operation = match self.rng.gen_range(0..6) {
            0 => Operation::InsertCustomer {
                name: Name(EN).fake_with_rng(&mut self.rng),
                address: StreetName(EN).fake_with_rng(&mut self.rng),
            },
            1 => {
                let customer_id = self.db.get_random_customer_id()?;

                Operation::InsertAccount {
                    customer_id,
                    account_type: ["Savings", "Checking", "Brokerage", "Investment"]
                        .choose(&mut self.rng)
                        .unwrap()
                        .to_string(),
                    balance: self.rng.gen_range(0.0..10000.0),
                    parent_account_id: None,
                }
            }
            2 => Operation::InsertSecurity {
                ticker: ticker(&mut self.rng),
                name: CompanyName(EN).fake_with_rng(&mut self.rng),
                sector: Industry(EN).fake_with_rng(&mut self.rng),
            },
            3 => {
                let account_id = self.db.get_random_account_id()?;
                let security_id = self.db.get_random_security_id()?;

                Operation::InsertTrade {
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

                Operation::InsertOrder {
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

                Operation::InsertMarketData {
                    security_id,
                    price: self.rng.gen_range(100.0..500.0),
                    volume: self.rng.gen_range(1000..100000),
                }
            }
        };

        Ok(operation)
    }

    fn generate_update(&mut self) -> Result<Operation> {
        let operation = match self.rng.gen_range(0..5) {
            0 => {
                let customer_id = self.db.get_random_customer_id()?;
                Operation::UpdateCustomer {
                    customer_id,
                    address: StreetName(EN).fake_with_rng(&mut self.rng),
                }
            }
            1 => {
                let account_id = self.db.get_random_account_id()?;
                Operation::UpdateAccount {
                    account_id,
                    balance: self.rng.gen_range(0.0..10000.0),
                }
            }
            2 => {
                let trade_id = self.db.get_random_trade_id()?;
                Operation::UpdateTrade {
                    trade_id,
                    price: self.rng.gen_range(100.0..500.0),
                }
            }
            3 => {
                let order_id = self.db.get_random_order_id()?;
                Operation::UpdateOrder {
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
                Operation::UpdateMarketData {
                    market_data_id,
                    price: self.rng.gen_range(100.0..500.0),
                    volume: self.rng.gen_range(1000..100000) as f64,
                }
            }
        };

        Ok(operation)
    }

    fn generate_delete(&mut self) -> Result<Operation> {
        let operation = match self.rng.gen_range(0..6) {
            0 => Operation::DeleteCustomer {
                customer_id: self.db.get_random_customer_id()?,
            },
            1 => Operation::DeleteAccount {
                account_id: self.db.get_random_account_id()?,
            },
            2 => Operation::DeleteSecurity {
                security_id: self.db.get_random_security_id()?,
            },
            3 => Operation::DeleteTrade {
                trade_id: self.db.get_random_trade_id()?,
            },
            4 => Operation::DeleteOrder {
                order_id: self.db.get_random_order_id()?,
            },
            _ => Operation::DeleteMarketData {
                market_data_id: self.db.get_random_market_data_id()?,
            },
        };

        Ok(operation)
    }
}

fn ticker<R: Rng + ?Sized>(rng: &mut R) -> String {
    (0..4).map(|_| rng.sample(Alphanumeric) as char).collect()
}
