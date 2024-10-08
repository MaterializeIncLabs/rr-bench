use anyhow::{Context, Result};
use rr_bench_base::benchmark;
use rr_bench_base::clap::{Arg, ArgMatches};
use rr_bench_base::operations::WriteOperation;
use rr_bench_base::{Benchmark, PrimaryDatabase, ReadReplica};
use rusqlite::{params, Connection};

fn main() {
    benchmark(
        || {
            [Arg::new("dbpath")
                .long("db-path")
                .required(true)
                .help("The path to the SQLite database file")]
        },
        SQLiteBenchmark::new,
    )
}

struct SQLiteBenchmark {
    dbpath: String,
}

struct SQLiteConnection {
    conn: Connection,
}

impl SQLiteBenchmark {
    fn new(matches: ArgMatches) -> Result<Self> {
        Ok(Self {
            dbpath: matches
                .get_one::<String>("dbpath")
                .context("missing required parameter db-path")?
                .to_string(),
        })
    }
}

impl Benchmark<'_> for SQLiteBenchmark {
    type Writer = SQLiteConnection;
    type Reader = SQLiteConnection;

    fn primary_database(&self) -> Result<Self::Writer> {
        SQLiteConnection::new(&self.dbpath)
    }

    fn read_replica(&self) -> Result<Self::Reader> {
        SQLiteConnection::new(&self.dbpath)
    }
}

impl SQLiteConnection {
    fn new(db: &str) -> Result<Self> {
        let conn = Connection::open(db).context("failed to open SQLite database")?;
        conn.pragma_update(None, "journal_mode", "WAL")
            .context("failed to enable WAL")?;

        Ok(Self { conn })
    }
}

impl PrimaryDatabase for SQLiteConnection {
    fn get_random_customer_id(&mut self) -> Result<i32> {
        self.conn
            .query_row(
                "SELECT customer_id FROM customers ORDER BY random() LIMIT 1",
                [],
                |row| row.get("customer_id"),
            )
            .context("failed to retrieve customer_id")
    }

    fn get_random_account_id(&mut self) -> Result<i32> {
        self.conn
            .query_row(
                "SELECT account_id FROM accounts ORDER BY random() LIMIT 1",
                [],
                |row| row.get("account_id"),
            )
            .context("failed to retrieve account_id")
    }

    fn get_random_security_id(&mut self) -> Result<i32> {
        self.conn
            .query_row(
                "SELECT security_id FROM securities ORDER BY random() LIMIT 1",
                [],
                |row| row.get("security_id"),
            )
            .context("failed to retrieve security_id")
    }

    fn get_random_trade_id(&mut self) -> Result<i32> {
        self.conn
            .query_row(
                "SELECT trade_id FROM trades ORDER BY random() LIMIT 1",
                [],
                |row| row.get("trade_id"),
            )
            .context("failed to retrieve trade_id")
    }

    fn get_random_order_id(&mut self) -> Result<i32> {
        self.conn
            .query_row(
                "SELECT order_id FROM orders ORDER BY random() LIMIT 1",
                [],
                |row| row.get("order_id"),
            )
            .context("failed to retrieve order_id")
    }

    fn get_random_market_data_id(&mut self) -> Result<i32> {
        self.conn
            .query_row(
                "SELECT market_data_id FROM market_data ORDER BY random() LIMIT 1",
                [],
                |row| row.get("market_data_id"),
            )
            .context("failed to retrieve trade_id")
    }

    fn get_random_ticker(&mut self) -> Result<String> {
        self.conn
            .query_row(
                "SELECT ticker FROM tickers ORDER BY random() LIMIT 1",
                [],
                |row| row.get("ticker"),
            )
            .context("failed to retrieve ticker")
    }

    fn get_random_sector(&mut self) -> Result<String> {
        self.conn
            .query_row(
                "SELECT sector FROM securities ORDER BY random() LIMIT 1",
                [],
                |row| row.get("sector"),
            )
            .context("failed to retrieve sector")
    }

    fn execute_command(&self, op: WriteOperation) -> Result<()> {
        match op {
            WriteOperation::InsertCustomer { name, address } => self.conn.execute(
                "INSERT INTO customers (name, address) VALUES (?1, ?2)", params![name, address])
                .map(|_| ())
                .context("failed to insert customer"),
            WriteOperation::InsertAccount { customer_id, account_type, balance, parent_account_id } => {
                match parent_account_id {
                    None => {
                        self.conn.execute("INSERT INTO accounts (customer_id, account_type, balance) VALUES (?1, ?2, ?3)", params![customer_id, account_type, balance])
                            .map(|_| ())
                            .context("failed to insert account")
                    }
                    Some(parent_account_id) => {
                        self.conn.execute("INSERT INTO accounts (customer_id, account_type, balance, parent_account_id) VALUES (?1, ?2, ?3, ?4)", params![customer_id, account_type, balance, parent_account_id])
                            .map(|_| ())
                            .context("failed to insert account")
                    }
                }
            }
            WriteOperation::InsertSecurity { ticker, name, sector } => {
                self.conn.execute("INSERT INTO securities (ticker, name, sector) VALUES (?1, ?2, ?3)", params![ticker, name, sector])
                    .map(|_| ())
                    .context("failed to insert security")
            }
            WriteOperation::InsertTrade { account_id, security_id, trade_type, quantity, price, parent_trade_id } => {
                match parent_trade_id {
                    None =>
                        self.conn.execute("INSERT INTO trades (account_id, security_id, trade_type, quantity, price) VALUES (?1, ?2, ?3, ?4, ?5)", params![account_id, security_id, trade_type, quantity, price])
                            .map(|_| ())
                            .context("failed to insert trades"),
                    Some(parent_trade_id) => self.conn.execute("INSERT INTO trades (account_id, security_id, trade_type, quantity, price, parent_trade_id) VALUES (?1, ?2, ?3, ?4, ?5, ?6)", params![account_id, security_id, trade_type, quantity, price, parent_trade_id])
                        .map(|_| ())
                        .context("failed to insert trades")
                }
            }
            WriteOperation::InsertOrder { account_id, security_id, order_type, quantity, limit_price,  status, parent_order_id} => {
                match parent_order_id  {
                    None => self.conn.execute("INSERT INTO orders (account_id, security_id, order_type, quantity, limit_price, status) VALUES (?1, ?2, ?3, ?4, ?5, ?6)", params![account_id, security_id, order_type, quantity, limit_price, status])
                        .map(|_| ())
                        .context("failed to insert order"),
                    Some(parent_order_id) => self.conn.execute("INSERT INTO orders (account_id, security_id, order_type, quantity, limit_price, status, parent_order_id) VALUES (?1, ?2, ?3, ?4, ?5, ?6)", params![account_id, security_id, order_type, quantity, limit_price, status, parent_order_id])
                        .map(|_| ())
                        .context("failed to insert order"),
                }
            }
            WriteOperation::InsertMarketData { security_id, price, volume } => self.conn.execute("INSERT INTO market_data (security_id, price, volume) VALUES (?1, ?2, ?3)", params![security_id, price, volume])
                .map(|_| ())
                .context("failed to insert market data"),
            WriteOperation::UpdateCustomer { customer_id, address } => self.conn.execute("UPDATE customers SET address = ?1 WHERE customer_id = ?2", params![address, customer_id])
                .map(|_| ())
                .context("failed to update customer"),
            WriteOperation::UpdateAccount { account_id, balance } => self.conn.execute("UPDATE accounts SET balance = ?1 WHERE customer_id = ?2",
                                                                                       params![balance, account_id])
                .map(|_| ())
                .context("failed to update account"),
            WriteOperation::UpdateTrade { trade_id, price } => self.conn.execute("UPDATE trades SET price = ?1 WHERE trade_id = ?2",
                                                                                 params![price, trade_id])
                .map(|_| ())
                .context("failed to update trades"),
            WriteOperation::UpdateOrder { order_id, status, limit_price } => self.conn.execute("UPDATE orders SET status = ?1, limit_price = ?2 WHERE order_id = ?3",
                                                                                               params![status, limit_price, order_id])
                .map(|_| ())
                .context("failed to update orders"),
            WriteOperation::UpdateMarketData { market_data_id, price, volume } => self.conn.execute("UPDATE market_data SET price = ?1, volume = ?2, market_date = CURRENT_TIMESTAMP WHERE market_data_id = ?3",
                                                                                                    params![price, volume, market_data_id])
                .map(|_| ())
                .context("failed to update market_data"),
            WriteOperation::DeleteCustomer { customer_id } => self.conn.execute("DELETE FROM customers WHERE customer_id = ?1", params![customer_id])
                .map(|_| ())
                .context("failed to delete customer"),
            WriteOperation::DeleteAccount { account_id } => self.conn.execute("DELETE FROM accounts WHERE account_id = ?1", params![account_id])
                .map(|_| ())
                .context("failed to delete accounts"),
            WriteOperation::DeleteSecurity { security_id } => self.conn.execute("DELETE FROM securities WHERE security_id = ?1", params![security_id])
                .map(|_| ())
                .context("failed to delete security"),
            WriteOperation::DeleteTrade { trade_id } => self.conn.execute("DELETE FROM trades WHERE trade_id = ?1", params![trade_id])
                .map(|_| ())
                .context("failed to delete trades"),
            WriteOperation::DeleteOrder { order_id } => self.conn.execute("DELETE FROM orders WHERE order_id = ?1", params![order_id])
                .map(|_| ())
                .context("failed to delete orders"),
            WriteOperation::DeleteMarketData { market_data_id } => self.conn.execute("DELETE FROM market_data WHERE market_data_id = ?1", params![market_data_id])
                .map(|_| ())
                .context("failed to delete market_data")
        }
    }
}

impl ReadReplica for SQLiteConnection {
    fn customer_portfolio(&mut self, customer_id: i32) -> Result<()> {
        let mut stmt = self
            .conn
            .prepare("SELECT * FROM customer_portfolio WHERE customer_id = ?1")
            .unwrap();

        stmt.query(params![customer_id])
            .map(|_| ())
            .with_context(|| format!("failed to query customer profile {customer_id}"))
    }

    fn top_performers(&mut self) -> Result<()> {
        let mut stmt = self.conn.prepare("SELECT * FROM top_performers").unwrap();
        stmt.query(params![])
            .map(|_| ())
            .with_context(|| "failed to query top_performers".to_string())
    }

    fn market_overview(&mut self, sector: &str) -> Result<()> {
        let mut stmt = self
            .conn
            .prepare("SELECT * FROM market_overview WHERE sector = ?1")
            .unwrap();
        stmt.query(params![sector])
            .map(|_| ())
            .with_context(|| "failed to query market_overview".to_string())
    }

    fn recent_large_trades(&mut self, account_id: i32) -> Result<()> {
        let mut stmt = self
            .conn
            .prepare("SELECT * FROM recent_large_trades WHERE account_id = ?1")
            .unwrap();
        stmt.query(params![account_id])
            .map(|_| ())
            .with_context(|| "failed to query recent_large_trades".to_string())
    }

    fn customer_order_book(&mut self, customer_id: i32) -> Result<()> {
        let mut stmt = self
            .conn
            .prepare("SELECT * FROM customer_order_book WHERE customer_id = ?1")
            .unwrap();
        stmt.query(params![customer_id])
            .map(|_| ())
            .with_context(|| format!("failed to query customer_order_book {customer_id}"))
    }

    fn sector_performance(&mut self, sector: String) -> Result<()> {
        let mut stmt = self
            .conn
            .prepare("SELECT * FROM sector_performance WHERE sector = ?1")
            .unwrap();
        stmt.query(params![sector])
            .map(|_| ())
            .with_context(|| "failed to query sector_performance".to_string())
    }

    fn account_activity_summary(&mut self, account_id: i32) -> Result<()> {
        let mut stmt = self
            .conn
            .prepare("SELECT * FROM account_activity_summary WHERE account_id = ?1")
            .unwrap();
        stmt.query(params![account_id])
            .map(|_| ())
            .with_context(|| format!("failed to query account_activity_summary {account_id}"))
    }

    fn daily_market_movements(&mut self, security_id: i32) -> Result<()> {
        let mut stmt = self
            .conn
            .prepare("SELECT * FROM daily_market_movements WHERE security_id = ?1")
            .unwrap();
        stmt.query(params![security_id])
            .map(|_| ())
            .with_context(|| format!("failed to query daily_market_movements {security_id}"))
    }

    fn high_value_customers(&mut self) -> Result<()> {
        let mut stmt = self
            .conn
            .prepare("SELECT * FROM high_value_customers")
            .unwrap();
        stmt.query(params![])
            .map(|_| ())
            .with_context(|| "failed to query high_value_customers".to_string())
    }

    fn pending_orders_summary(&mut self, ticker: &str) -> Result<()> {
        let mut stmt = self
            .conn
            .prepare("SELECT * FROM pending_orders_summary WHERE ticker = ?1")
            .unwrap();
        stmt.query(params![ticker])
            .map(|_| ())
            .with_context(|| "failed to query pending_orders_summary".to_string())
    }

    fn trade_volume_by_hour(&mut self) -> Result<()> {
        let mut stmt = self
            .conn
            .prepare("SELECT * FROM trade_volume_by_hour")
            .unwrap();
        stmt.query(params![])
            .map(|_| ())
            .with_context(|| "failed to query trade_volume_by_hour".to_string())
    }

    fn top_securities_by_sector(&mut self, sector: String) -> Result<()> {
        let mut stmt = self
            .conn
            .prepare("SELECT * FROM top_securities_by_sector WHERE sector = ?1")
            .unwrap();
        stmt.query(params![sector])
            .map(|_| ())
            .with_context(|| "failed to query top_securities_by_sector".to_string())
    }

    fn recent_trades_by_account(&mut self, account_id: i32) -> Result<()> {
        let mut stmt = self
            .conn
            .prepare("SELECT * FROM recent_trades_by_account WHERE account_id = ?1")
            .unwrap();
        stmt.query(params![account_id])
            .map(|_| ())
            .with_context(|| format!("failed to query recent_trades_by_account {account_id}"))
    }

    fn order_fulfillment_rates(&mut self, customer_id: i32) -> Result<()> {
        let mut stmt = self
            .conn
            .prepare("SELECT * FROM order_fulfillment_rates WHERE customer_id = ?1")
            .unwrap();
        stmt.query(params![customer_id])
            .map(|_| ())
            .with_context(|| format!("failed to query order_fulfillment_rates {customer_id}"))
    }

    fn sector_order_activity(&mut self, sector: String) -> Result<()> {
        let mut stmt = self
            .conn
            .prepare("SELECT * FROM sector_order_activity WHERE sector = ?1")
            .unwrap();
        stmt.query(params![sector])
            .map(|_| ())
            .with_context(|| "failed to query sector_order_activity".to_string())
    }

    fn cascading_order_cancellation_alert(&mut self) -> Result<()> {
        let mut stmt = self
            .conn
            .prepare("SELECT * FROM cascading_order_cancellation_alert")
            .unwrap();
        stmt.query(params![])
            .map(|_| ())
            .with_context(|| "failed to query cascading_order_cancellation_alert".to_string())
    }
}
