use anyhow::{Context, Result};
use clap::Parser;
use fake::faker::address::raw::StreetName;
use fake::faker::company::raw::{CompanyName, Industry};
use fake::faker::name::raw::Name;
use fake::locales::EN;
use fake::Fake;
use indicatif::{ProgressBar, ProgressStyle};
use rand::distributions::Alphanumeric;
use rand::prelude::{SliceRandom, StdRng};
use rand::{thread_rng, Rng, SeedableRng};
use rusqlite::functions::FunctionFlags;
use rusqlite::{params, Connection, Transaction};
use serde::Serialize;
use std::env;
use std::fs::File;
use std::num::{NonZero, NonZeroU8};
use std::path::PathBuf;
use uuid::Uuid;

const GIGABYTE: u64 = 1024 * 1024 * 1024;

#[derive(Parser, Debug)]
struct Cli {
    #[clap(long, default_value_t = NonZero::new(1).unwrap())]
    scale: NonZeroU8,

    #[clap(long)]
    seed: Option<u64>,

    #[clap(long, default_value = "data/")]
    target: PathBuf,
}

#[derive(Serialize)]
struct Customer {
    customer_id: i64,
    name: String,
    address: Option<String>,
    created_at: String,
}

#[derive(Serialize)]
struct Account {
    account_id: i64,
    customer_id: i64,
    account_type: String,
    balance: f64,
    created_at: String,
}

#[derive(Serialize)]
struct Security {
    security_id: i64,
    ticker: String,
    name: Option<String>,
    sector: Option<String>,
    created_at: String,
}

#[derive(Serialize)]
struct Trade {
    trade_id: i64,
    account_id: i64,
    security_id: i64,
    trade_type: String,
    quantity: i32,
    price: f64,
    trade_date: String,
}

#[derive(Serialize)]
struct Order {
    order_id: i64,
    account_id: i64,
    security_id: i64,
    order_type: String,
    quantity: i32,
    limit_price: Option<f64>,
    status: String,
    order_date: String,
}

#[derive(Serialize)]
struct MarketData {
    market_data_id: i64,
    security_id: i64,
    price: f64,
    volume: i32,
    market_date: String,
}

fn get_db_size(conn: &mut Connection) -> u64 {
    let page_count: u64 = conn
        .query_row("PRAGMA page_count;", [], |row| row.get(0))
        .expect("failed to query page_count");
    let page_size: u64 = conn
        .query_row("PRAGMA page_size;", [], |row| row.get(0))
        .expect("failed to query page_size");

    page_count * page_size
}

fn get_random_ids(tx: &Transaction, table: &str, column: &str, num: usize) -> Vec<i64> {
    let query = format!(
        "SELECT {} FROM {} ORDER BY my_random() LIMIT {}",
        column, table, num
    );
    let mut stmt = tx.prepare(&query).expect("failed to prepare statement");
    let mut rows = stmt
        .query([]) //, |row| row.get(0))
        .unwrap_or_else(|_| panic!("failed to retrieve random {num} {column} from {table}"));

    let mut ids = vec![];
    while let Some(row) = rows.next().unwrap() {
        ids.push(row.get(0).unwrap())
    }

    ids
}

struct Generator {
    rng: StdRng,
}

impl Generator {
    fn populate_customers(
        &mut self,
        conn: &mut Connection,
        batch_size: usize,
    ) -> rusqlite::Result<()> {
        let tx = conn.transaction()?;
        for _ in 0..batch_size {
            let name: String = Name(EN).fake_with_rng(&mut self.rng);
            let address: String = StreetName(EN).fake_with_rng(&mut self.rng);
            tx.execute(
                "INSERT INTO customers (name, address) VALUES (?, ?);",
                params![name, address],
            )?;
        }
        tx.commit()?;
        Ok(())
    }

    fn populate_accounts(
        &mut self,
        conn: &mut Connection,
        batch_size: usize,
    ) -> rusqlite::Result<()> {
        let tx = conn.transaction()?;
        let customer_ids = get_random_ids(&tx, "customers", "customer_id", batch_size);
        for customer_id in customer_ids {
            let account_type = ["Savings", "Checking", "Brokerage", "Investment"]
                .choose(&mut self.rng)
                .unwrap()
                .to_string();
            let balance = self.rng.gen_range(0.0..10000.0);
            tx.execute(
                "INSERT INTO accounts (customer_id, account_type, balance) VALUES (?, ?, ?);",
                params![customer_id, account_type, balance],
            )?;
        }
        tx.commit()?;
        Ok(())
    }

    fn populate_securities(
        &mut self,
        conn: &mut Connection,
        batch_size: usize,
    ) -> rusqlite::Result<()> {
        let tx = conn.transaction()?;
        for _ in 0..batch_size {
            let ticker = ticker(&mut self.rng);
            let name: String = CompanyName(EN).fake_with_rng(&mut self.rng);
            let sector: String = Industry(EN).fake_with_rng(&mut self.rng);
            tx.execute(
                "INSERT INTO securities (ticker, name, sector) VALUES (?, ?, ?);",
                params![ticker, name, sector],
            )?;
        }
        tx.commit()?;
        Ok(())
    }

    fn populate_trades(
        &mut self,
        conn: &mut Connection,
        batch_size: usize,
    ) -> rusqlite::Result<()> {
        let tx = conn.transaction()?;
        let account_ids = get_random_ids(&tx, "accounts", "account_id", batch_size);
        let security_ids = get_random_ids(&tx, "securities", "security_id", batch_size);

        let ids = account_ids.iter().zip(security_ids);

        for (account_id, security_id) in ids {
            let trade_type = ["buy", "sell"].choose(&mut self.rng).unwrap().to_string();
            let quantity = self.rng.gen_range(1..1000);
            let price = self.rng.gen_range(100.0..500.0);
            tx.execute(
            "INSERT INTO trades (account_id, security_id, trade_type, quantity, price) VALUES (?, ?, ?, ?, ?);",
            params![account_id, security_id, trade_type, quantity, price],
        )?;
        }
        tx.commit()?;
        Ok(())
    }

    fn populate_orders(
        &mut self,
        conn: &mut Connection,
        batch_size: usize,
    ) -> rusqlite::Result<()> {
        let tx = conn.transaction()?;
        let account_ids = get_random_ids(&tx, "accounts", "account_id", batch_size);
        let security_ids = get_random_ids(&tx, "securities", "security_id", batch_size);

        let ids = account_ids.iter().zip(security_ids);

        for (account_id, security_id) in ids {
            let order_type = ["buy", "sell"].choose(&mut self.rng).unwrap().to_string();
            let quantity = self.rng.gen_range(1..1000);
            let limit_price = self.rng.gen_range(1..1000) as f64;
            let status = ["pending", "completed", "canceled"]
                .choose(&mut self.rng)
                .unwrap()
                .to_string();
            tx.execute(
            "INSERT INTO orders (account_id, security_id, order_type, quantity, limit_price, status) VALUES (?, ?, ?, ?, ?, ?);",
            params![account_id, security_id, order_type, quantity, limit_price, status],
        )?;
        }
        tx.commit()?;
        Ok(())
    }

    fn populate_market_data(
        &mut self,
        conn: &mut Connection,
        batch_size: usize,
    ) -> rusqlite::Result<()> {
        let tx = conn.transaction()?;
        let security_ids = get_random_ids(&tx, "securities", "security_id", batch_size);

        for security_id in security_ids {
            let price = self.rng.gen_range(100.0..500.0);
            let volume = self.rng.gen_range(1000..100000);
            tx.execute(
                "INSERT INTO market_data (security_id, price, volume) VALUES (?, ?, ?);",
                params![security_id, price, volume],
            )?;
        }
        tx.commit()?;
        Ok(())
    }

    fn populate_database(&mut self, conn: &mut Connection, target_size_gb: u8) -> Result<()> {
        let target_size_bytes = target_size_gb as u64 * GIGABYTE;
        let batch_size = 1000;

        let progress_bar = ProgressBar::new(target_size_bytes);
        progress_bar.set_style(
            ProgressStyle::default_bar()
                .template("{wide_bar} {bytes}/{total_bytes} ({eta})")
                .unwrap()
                .progress_chars("#>-"),
        );

        let mut last_size = 0;
        loop {
            let current_size = get_db_size(conn);
            let diff = current_size - last_size;
            progress_bar.inc(diff);
            last_size = current_size;

            if current_size >= target_size_bytes {
                break;
            }

            self.populate_customers(conn, batch_size)
                .context("failed to populate customers")?;
            self.populate_accounts(conn, batch_size * 2)
                .context("failed to populate accounts")?;
            self.populate_securities(conn, batch_size * 3)
                .context("failed to populate securities")?;
            self.populate_trades(conn, batch_size * 10)
                .context("failed to populate trades")?;
            self.populate_orders(conn, batch_size * 8)
                .context("failed to populate orders")?;
            self.populate_market_data(conn, batch_size * 10)
                .context("failed to populate market_data")?;
        }

        progress_bar.finish();
        Ok(())
    }
}

fn export_to_csv<T: Serialize>(
    conn: &mut Connection,
    query: &str,
    file_name: &str,
    map_fn: fn(&rusqlite::Row) -> Result<T>,
) -> Result<()> {
    let mut stmt = conn.prepare(query).context("failed to prepare query")?;
    let mut rows = stmt.query([]).unwrap();
    let mut wtr =
        csv::Writer::from_writer(File::create(file_name).context("failed to create file")?);

    while let Some(row) = rows.next().context("failed to get next row")? {
        let record = map_fn(row).context("failed to map row")?;
        wtr.serialize(record)
            .context("failed to serialize record")?;
    }

    wtr.flush().unwrap();
    Ok(())
}

fn ticker<R: Rng + ?Sized>(rng: &mut R) -> String {
    (0..4).map(|_| rng.sample(Alphanumeric) as char).collect()
}

// Helper functions to map database rows to Rust structs
fn map_customer(row: &rusqlite::Row) -> Result<Customer> {
    Ok(Customer {
        customer_id: row.get(0)?,
        name: row.get(1)?,
        address: row.get(2)?,
        created_at: row.get(3)?,
    })
}

fn map_account(row: &rusqlite::Row) -> Result<Account> {
    Ok(Account {
        account_id: row.get(0)?,
        customer_id: row.get(1)?,
        account_type: row.get(2)?,
        balance: row.get(3)?,
        created_at: row.get(4)?,
    })
}

fn map_security(row: &rusqlite::Row) -> Result<Security> {
    Ok(Security {
        security_id: row.get(0)?,
        ticker: row.get(1)?,
        name: row.get(2)?,
        sector: row.get(3)?,
        created_at: row.get(4)?,
    })
}

fn map_trade(row: &rusqlite::Row) -> Result<Trade> {
    Ok(Trade {
        trade_id: row.get(0)?,
        account_id: row.get(1)?,
        security_id: row.get(2)?,
        trade_type: row.get(3)?,
        quantity: row.get(4)?,
        price: row.get(5)?,
        trade_date: row.get(6)?,
    })
}

fn map_order(row: &rusqlite::Row) -> Result<Order> {
    Ok(Order {
        order_id: row.get(0)?,
        account_id: row.get(1)?,
        security_id: row.get(2)?,
        order_type: row.get(3)?,
        quantity: row.get(4)?,
        limit_price: row.get(5)?,
        status: row.get(6)?,
        order_date: row.get(7)?,
    })
}

fn map_market_data(row: &rusqlite::Row) -> Result<MarketData> {
    Ok(MarketData {
        market_data_id: row.get(0)?,
        security_id: row.get(1)?,
        price: row.get(2)?,
        volume: row.get(3)?,
        market_date: row.get(4)?,
    })
}

fn main() -> Result<()> {
    let cli: Cli = Cli::parse();

    let seed = cli.seed.unwrap_or_else(|| thread_rng().gen());
    println!("Generating {} gb of data with seed {}", cli.scale, seed);

    if !cli.target.exists() {
        println!("Directory does not exist. Creating: {:?}", cli.target);
        if let Err(e) = std::fs::create_dir_all(&cli.target) {
            eprintln!("Failed to create directory: {:?}", e);
            std::process::exit(1);
        }
    }

    let mut temp_path: PathBuf = env::temp_dir();
    let unique_filename = format!("rr_data_gen_{}.db", Uuid::new_v4());
    temp_path.push(unique_filename);
    let mut conn = Connection::open(temp_path).expect("failed to open database");

    conn.execute_batch(
        "
        CREATE TABLE customers (
            customer_id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            address TEXT,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP
        );
        CREATE TABLE accounts (
            account_id INTEGER PRIMARY KEY AUTOINCREMENT,
            customer_id INTEGER,
            account_type TEXT NOT NULL,
            balance DECIMAL(18, 2) NOT NULL,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (customer_id) REFERENCES customers(customer_id) ON DELETE CASCADE
        );
        CREATE TABLE securities (
            security_id INTEGER PRIMARY KEY AUTOINCREMENT,
            ticker TEXT NOT NULL,
            name TEXT,
            sector TEXT,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP
        );
        CREATE TABLE trades (
            trade_id INTEGER PRIMARY KEY AUTOINCREMENT,
            account_id INTEGER,
            security_id INTEGER,
            trade_type TEXT NOT NULL CHECK (trade_type IN ('buy', 'sell')),
            quantity INTEGER NOT NULL,
            price DECIMAL(18, 4) NOT NULL,
            trade_date DATETIME DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (account_id) REFERENCES accounts(account_id) ON DELETE CASCADE,
            FOREIGN KEY (security_id) REFERENCES securities(security_id) ON DELETE CASCADE
        );
        CREATE TABLE orders (
            order_id INTEGER PRIMARY KEY AUTOINCREMENT,
            account_id INTEGER,
            security_id INTEGER,
            order_type TEXT NOT NULL CHECK (order_type IN ('buy', 'sell')),
            quantity INTEGER NOT NULL,
            limit_price DECIMAL(18, 4),
            status TEXT NOT NULL CHECK (status IN ('pending', 'completed', 'canceled')),
            order_date DATETIME DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (account_id) REFERENCES accounts(account_id) ON DELETE CASCADE,
            FOREIGN KEY (security_id) REFERENCES securities(security_id) ON DELETE CASCADE
        );
        CREATE TABLE market_data (
            market_data_id INTEGER PRIMARY KEY AUTOINCREMENT,
            security_id INTEGER,
            price DECIMAL(18, 4) NOT NULL,
            volume INTEGER NOT NULL,
            market_date DATETIME DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (security_id) REFERENCES securities(security_id) ON DELETE CASCADE
        );
        ",
    )?;

    // Define our own random function
    // to keep data generation deterministic
    let mut sqlite_rng = StdRng::seed_from_u64(seed);

    conn.create_scalar_function("my_random", 0, FunctionFlags::SQLITE_UTF8, move |_| {
        Ok(sqlite_rng.gen::<i64>())
    })?;

    let mut generator = Generator {
        rng: StdRng::seed_from_u64(seed),
    };
    generator.populate_database(&mut conn, cli.scale.get())?;

    // Export to CSV
    export_to_csv::<Customer>(
        &mut conn,
        "SELECT * FROM customers",
        "customers.csv",
        map_customer,
    )?;
    export_to_csv::<Account>(
        &mut conn,
        "SELECT * FROM accounts",
        "accounts.csv",
        map_account,
    )?;
    export_to_csv::<Security>(
        &mut conn,
        "SELECT * FROM securities",
        "securities.csv",
        map_security,
    )?;
    export_to_csv::<Trade>(&mut conn, "SELECT * FROM trades", "trades.csv", map_trade)?;
    export_to_csv::<Order>(&mut conn, "SELECT * FROM orders", "orders.csv", map_order)?;
    export_to_csv::<MarketData>(
        &mut conn,
        "SELECT * FROM market_data",
        "market_data.csv",
        map_market_data,
    )?;

    println!("CSV files generated.");

    Ok(())
}
