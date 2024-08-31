use fake::faker::address::raw::StreetName;
use fake::faker::company::raw::{CompanyName, Industry};
use fake::faker::name::raw::Name;
use fake::locales::EN;
use fake::Fake;
use rand::distributions::Alphanumeric;
use rand::prelude::{SliceRandom, StdRng};
use rand::{Rng, SeedableRng};

pub struct DataGenerator {
    rng: StdRng,
}

impl DataGenerator {
    pub fn new(seed: u64) -> Self {
        Self {
            rng: StdRng::seed_from_u64(seed),
        }
    }

    pub fn generate_customer(&mut self) -> Customer {
        Customer {
            name: self.generate_name(),
            address: self.generate_address(),
        }
    }

    pub fn generate_account(&mut self) -> Account {
        Account {
            account_type: self.generate_account_type(),
            balance: self.rng.gen_range(0.0..10000.0),
        }
    }

    pub fn generate_security(&mut self) -> Security {
        Security {
            ticker: self.generate_ticker(),
            name: self.generate_company_name(),
            sector: self.generate_industry(),
        }
    }

    pub fn generate_trade(&mut self) -> Trade {
        Trade {
            trade_type: self.generate_trade_type(),
            quantity: self.rng.gen_range(1..1000),
            price: self.rng.gen_range(100.0..500.0),
        }
    }

    pub fn generate_order(&mut self) -> Order {
        Order {
            order_type: self.generate_order_type(),
            quantity: self.rng.gen_range(1..1000),
            limit_price: self.rng.gen_range(1..1000) as f64,
            status: self.generate_status(),
        }
    }

    pub fn generate_market_data(&mut self) -> MarketData {
        MarketData {
            price: self.rng.gen_range(100.0..500.0),
            volume: self.rng.gen_range(1000..100000),
        }
    }

    fn generate_name(&mut self) -> String {
        Name(EN).fake_with_rng(&mut self.rng)
    }

    fn generate_address(&mut self) -> String {
        StreetName(EN).fake_with_rng(&mut self.rng)
    }

    fn generate_company_name(&mut self) -> String {
        CompanyName(EN).fake_with_rng(&mut self.rng)
    }

    fn generate_industry(&mut self) -> String {
        Industry(EN).fake_with_rng(&mut self.rng)
    }

    fn generate_ticker(&mut self) -> String {
        (0..4)
            .map(|_| self.rng.sample(Alphanumeric) as char)
            .collect()
    }

    fn generate_trade_type(&mut self) -> String {
        ["buy", "sell"].choose(&mut self.rng).unwrap().to_string()
    }

    fn generate_order_type(&mut self) -> String {
        ["buy", "sell"].choose(&mut self.rng).unwrap().to_string()
    }

    fn generate_account_type(&mut self) -> String {
        ["Savings", "Checking", "Brokerage", "Investment"]
            .choose(&mut self.rng)
            .unwrap()
            .to_string()
    }

    fn generate_status(&mut self) -> String {
        ["pending", "completed", "canceled"]
            .choose(&mut self.rng)
            .unwrap()
            .to_string()
    }
}

pub struct Customer {
    pub name: String,
    pub address: String,
}

pub struct Account {
    pub account_type: String,
    pub balance: f64,
}

pub struct Security {
    pub ticker: String,
    pub name: String,
    pub sector: String,
}

pub struct Trade {
    pub trade_type: String,
    pub quantity: i32,
    pub price: f64,
}

pub struct Order {
    pub order_type: String,
    pub quantity: i32,
    pub limit_price: f64,
    pub status: String,
}

pub struct MarketData {
    pub price: f64,
    pub volume: i32,
}
