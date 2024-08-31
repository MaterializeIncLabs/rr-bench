use anyhow::{Context, Result};
use openssl::ssl::{SslConnector, SslMethod, SslVerifyMode};
use pg_bigdecimal::{BigDecimal, PgNumeric};
use postgres::Client;
use postgres_openssl::MakeTlsConnector;
use r2d2_postgres::r2d2::Pool;
use r2d2_postgres::PostgresConnectionManager;
use rr_bench_core::clap::{Arg, ArgMatches};
use rr_bench_core::operations::WriteOperation;
use rr_bench_core::{benchmark, Benchmark, PrimaryDatabase, ReadReplica};

fn main() {
    benchmark(
        || {
            [
                Arg::new("writer")
                    .long("writer-url")
                    .required(true)
                    .help("The URL to the writer node"),
                Arg::new("reader")
                    .long("reader-url")
                    .required(true)
                    .help("The URL to the reader node"),
            ]
        },
        PostgresBenchmark::new,
    )
}

struct PostgresBenchmark {
    reader_url: String,
    pool: Pool<PostgresConnectionManager<MakeTlsConnector>>,
}

impl PostgresBenchmark {
    fn new(args: ArgMatches) -> Result<Self> {
        let writer = args
            .get_one::<String>("writer")
            .context("missing required argument writer-url")?
            .to_string();

        let reader_url = args
            .get_one::<String>("reader")
            .context("missing required argument writer-url")?
            .to_string();

        let mut builder =
            SslConnector::builder(SslMethod::tls()).context("Error creating ssl builder")?;
        builder.set_verify(SslVerifyMode::NONE);
        let tls = MakeTlsConnector::new(builder.build());

        let manager = PostgresConnectionManager::new(writer.parse().unwrap(), tls);
        let pool = Pool::new(manager).context("failed to create connection pool")?;

        Ok(Self { reader_url, pool })
    }
}

impl Benchmark<'_> for PostgresBenchmark {
    type Writer = PostgresPooledClient;
    type Reader = PostgresClient;

    fn primary_database(&self) -> Result<Self::Writer> {
        Ok(PostgresPooledClient {
            pool: self.pool.clone(),
        })
    }

    fn read_replica(&self) -> Result<Self::Reader> {
        PostgresClient::from_url(&self.reader_url)
    }
}

struct PostgresPooledClient {
    pool: Pool<PostgresConnectionManager<MakeTlsConnector>>,
}

struct PostgresClient {
    client: Client,
}

impl PostgresClient {
    fn from_url(url: &str) -> Result<Self> {
        let mut builder =
            SslConnector::builder(SslMethod::tls()).context("Error creating ssl builder")?;
        builder.set_verify(SslVerifyMode::NONE);
        let tls = MakeTlsConnector::new(builder.build());
        let client = Client::connect(url, tls)
            .context("failed to open postgres client to primary database")?;
        Ok(PostgresClient { client })
    }
}

impl PrimaryDatabase for PostgresPooledClient {
    fn get_random_customer_id(&mut self) -> Result<i32> {
        self.pool
            .get()
            .context("failed to acquire connection from pool")?
            .query_one(
                "SELECT customer_id FROM customers ORDER BY random() LIMIT 1",
                &[],
            )
            .context("failed to query customer id")
            .map(|row| row.get::<_, i32>("customer_id"))
    }

    fn get_random_account_id(&mut self) -> Result<i32> {
        self.pool
            .get()
            .context("failed to acquire connection from pool")?
            .query_one(
                "SELECT account_id FROM accounts ORDER BY random() LIMIT 1",
                &[],
            )
            .context("failed to query account id")
            .map(|row| row.get::<_, i32>("account_id"))
    }

    fn get_random_security_id(&mut self) -> Result<i32> {
        self.pool
            .get()
            .context("failed to acquire connection from pool")?
            .query_one(
                "SELECT security_id FROM securities ORDER BY random() LIMIT 1",
                &[],
            )
            .context("failed to query security id")
            .map(|row| row.get::<_, i32>("security_id"))
    }

    fn get_random_trade_id(&mut self) -> Result<i32> {
        self.pool
            .get()
            .context("failed to acquire connection from pool")?
            .query_one("SELECT trade_id FROM trades ORDER BY random() LIMIT 1", &[])
            .context("failed to query trade id")
            .map(|row| row.get::<_, i32>("trade_id"))
    }

    fn get_random_order_id(&mut self) -> Result<i32> {
        self.pool
            .get()
            .context("failed to acquire connection from pool")?
            .query_one("SELECT order_id FROM orders ORDER BY random() LIMIT 1", &[])
            .context("failed to query order id")
            .map(|row| row.get::<_, i32>("order_id"))
    }

    fn get_random_market_data_id(&mut self) -> Result<i32> {
        self.pool
            .get()
            .context("failed to acquire connection from pool")?
            .query_one(
                "SELECT market_data_id FROM market_data ORDER BY random() LIMIT 1",
                &[],
            )
            .context("failed to query market data id")
            .map(|row| row.get::<_, i32>("market_data_id"))
    }

    fn get_random_ticker(&mut self) -> Result<String> {
        self.pool
            .get()
            .context("failed to acquire connection from pool")?
            .query_one(
                "SELECT ticker FROM securities ORDER BY random() LIMIT 1",
                &[],
            )
            .context("failed to query sector")
            .map(|row| row.get("ticker"))
    }

    fn get_random_sector(&mut self) -> Result<String> {
        self.pool
            .get()
            .context("failed to acquire connection from pool")?
            .query_one(
                "SELECT sector FROM securities ORDER BY random() LIMIT 1",
                &[],
            )
            .context("failed to query sector")
            .map(|row| row.get("sector"))
    }

    fn execute_command(&self, op: WriteOperation) -> Result<()> {
        let mut client = self
            .pool
            .get()
            .context("failed to acquire connection from pool")?;
        match op {
            WriteOperation::InsertCustomer { name, address } => client.execute(
                "INSERT INTO customers (name, address) VALUES ($1, $2)", &[&name, &address])
                .map(|_| ())
                .context("failed to insert customer"),
            WriteOperation::InsertAccount { customer_id, account_type, balance, parent_account_id } => {
                match parent_account_id {
                    None => {
                        client.execute("INSERT INTO accounts (customer_id, account_type, balance) VALUES ($1, $2, $3)", &[&customer_id, &account_type, &PgNumeric::new(Some(BigDecimal::try_from(balance).unwrap()))])
                            .map(|_| ())
                            .context("failed to insert account")
                    }
                    Some(parent_account_id) => {
                        client.execute("INSERT INTO accounts (customer_id, account_type, balance, parent_account_id) VALUES ($1, $2, $3, $4)", &[&customer_id, &account_type, &PgNumeric::new(Some(BigDecimal::try_from(balance).unwrap())), &parent_account_id])
                            .map(|_| ())
                            .context("failed to insert account")
                    }
                }
            },
            WriteOperation::InsertSecurity { ticker, name, sector } => {
                client.execute("INSERT INTO securities (ticker, name, sector) VALUES ($1, $2, $3)", &[&ticker, &name, &sector])
                    .map(|_| ())
                    .context("failed to insert security")
            },

            WriteOperation::InsertTrade { account_id, security_id, trade_type, quantity, price, parent_trade_id } => {
                match parent_trade_id {
                    None =>
                        client.execute("INSERT INTO trades (account_id, security_id, trade_type, quantity, price) VALUES ($1, $2, $3, $4, $5)", &[&account_id, &security_id, &trade_type, &quantity, &PgNumeric::new(Some(BigDecimal::try_from(price).unwrap()))])
                            .map(|_| ())
                            .context("failed to insert trades"),
                    Some(parent_trade_id) => client.execute("INSERT INTO trades (account_id, security_id, trade_type, quantity, price, parent_trade_id) VALUES ($1, $2, $3, $4, $5, $6)", &[&account_id, &security_id, &trade_type, &quantity, &PgNumeric::new(Some(BigDecimal::try_from(price).unwrap())), &parent_trade_id])
                        .map(|_| ())
                        .context("failed to insert trades")
                }
            },

            WriteOperation::InsertOrder { account_id, security_id, order_type, quantity, limit_price,  status, parent_order_id} => {
                match parent_order_id  {
                    None => client
                        .execute("INSERT INTO orders (account_id, security_id, order_type, quantity, limit_price, status) VALUES ($1, $2, $3, $4, $5, $6)",
                                           &[&account_id, &security_id, &order_type, &quantity, &PgNumeric::new(Some(BigDecimal::try_from(limit_price).unwrap())), &status])
                        .map(|_| ())
                        .context("failed to insert order"),
                    Some(parent_order_id) => client
                        .execute("INSERT INTO orders (account_id, security_id, order_type, quantity, limit_price, status, parent_order_id) VALUES ($1, $2, $3, $4, $5, $6)",
                                 &[&account_id, &security_id, &order_type, &quantity, &PgNumeric::new(Some(BigDecimal::try_from(limit_price).unwrap())), &status, &parent_order_id])
                        .map(|_| ())
                        .context("failed to insert order"),
                }
            },
            WriteOperation::InsertMarketData { security_id, price, volume } => client
                .execute("INSERT INTO market_data (security_id, price, volume) VALUES ($1, $2, $3)",
                         &[&security_id, &PgNumeric::new(Some(BigDecimal::try_from(price).unwrap())), &volume])
                .map(|_| ())
                .context("failed to insert market data"),
            WriteOperation::UpdateCustomer { customer_id, address } => client
                .execute("UPDATE customers SET address = $1 WHERE customer_id = $2",&[&address, &customer_id])
                .map(|_| ())
                .context("failed to update customer"),
            WriteOperation::UpdateAccount { account_id, balance } => client
                .execute("UPDATE accounts SET balance = $1 WHERE customer_id = $2", &[
                    &PgNumeric::new(Some(BigDecimal::try_from(balance).unwrap())),
                    &account_id
                ]).map(|_| ())
                .context("failed to update account"),
            WriteOperation::UpdateTrade { trade_id, price } => client
                .execute("UPDATE trades SET price = $1 WHERE trade_id = $2", &[
                        &PgNumeric::new(Some(BigDecimal::try_from(price).unwrap())),
                    &trade_id
                ]).map(|_| ())
                .context("failed to update trades"),
            WriteOperation::UpdateOrder { order_id, status, limit_price } => client
                .execute("UPDATE orders SET status = $1, limit_price = $2 WHERE order_id = $3",&[
                        &status,
                        &PgNumeric::new(Some(BigDecimal::try_from(limit_price).unwrap())),
                        &order_id
                ]).map(|_| ())
                .context("failed to update orders"),
            WriteOperation::UpdateMarketData { .. } => Ok(()),/*client
                .execute("UPDATE market_data SET price = $1, volume = $2, market_date = CURRENT_TIMESTAMP WHERE market_data_id = $3", &[
                            &PgNumeric::new(Some(BigDecimal::try_from(price).unwrap())),
                            &PgNumeric::new(Some(BigDecimal::try_from(volume).unwrap())),
                            &market_data_id
                ]).map(|_| ())
                .context("failed to update market_data"),*/
            WriteOperation::DeleteCustomer { customer_id } => client
                .execute("DELETE FROM customers WHERE customer_id = $1", &[&customer_id])
                .map(|_| ())
                .context("failed to delete customer"),
            WriteOperation::DeleteAccount { account_id } => client.execute("DELETE FROM accounts WHERE account_id = $1", &[&account_id])
                .map(|_| ())
                .context("failed to delete accounts"),
            WriteOperation::DeleteSecurity { security_id } => client.execute("DELETE FROM securities WHERE security_id = $1", &[&security_id])
                .map(|_| ())
                .context("failed to delete security"),
            WriteOperation::DeleteTrade { trade_id } => client.execute("DELETE FROM trades WHERE trade_id = $1", &[&trade_id])
                .map(|_| ())
                .context("failed to delete trades"),
            WriteOperation::DeleteOrder { order_id } => client.execute("DELETE FROM orders WHERE order_id = $1", &[&order_id])
                .map(|_| ())
                .context("failed to delete orders"),
            WriteOperation::DeleteMarketData { market_data_id } => client.execute("DELETE FROM market_data WHERE market_data_id = $1", &[&market_data_id])
                .map(|_| ())
                .context("failed to delete market_data")
        }
    }
}

impl ReadReplica for PostgresClient {
    fn customer_portfolio(&mut self, customer_id: i32) -> Result<()> {
        self.client
            .query(
                "SELECT * FROM customer_portfolio WHERE customer_id = $1",
                &[&customer_id],
            )
            .map(|_| ())
            .with_context(|| format!("failed to query customer profile {customer_id}"))
    }

    fn top_performers(&mut self) -> Result<()> {
        self.client
            .query("SELECT * FROM top_performers", &[])
            .map(|_| ())
            .with_context(|| "failed to query top_performers".to_string())
    }

    fn market_overview(&mut self, sector: &str) -> Result<()> {
        self.client
            .query(
                "SELECT * FROM market_overview WHERE sector = $1",
                &[&sector],
            )
            .map(|_| ())
            .with_context(|| "failed to query market_overview".to_string())
    }

    fn recent_large_trades(&mut self, account_id: i32) -> Result<()> {
        self.client
            .query(
                "SELECT * FROM recent_large_trades WHERE account_id = $1",
                &[&account_id],
            )
            .map(|_| ())
            .with_context(|| "failed to query recent_large_trades".to_string())
    }

    fn customer_order_book(&mut self, customer_id: i32) -> Result<()> {
        self.client
            .query(
                "SELECT * FROM customer_order_book WHERE customer_id = $1",
                &[&customer_id],
            )
            .map(|_| ())
            .with_context(|| format!("failed to query customer_order_book {customer_id}"))
    }

    fn sector_performance(&mut self, sector: String) -> Result<()> {
        self.client
            .query(
                "SELECT * FROM sector_performance WHERE sector = $1",
                &[&sector],
            )
            .map(|_| ())
            .with_context(|| "failed to query sector_performance".to_string())
    }

    fn account_activity_summary(&mut self, account_id: i32) -> Result<()> {
        self.client
            .query(
                "SELECT * FROM account_activity_summary WHERE account_id = $1",
                &[&account_id],
            )
            .map(|_| ())
            .with_context(|| format!("failed to query account_activity_summary {account_id}"))
    }

    fn daily_market_movements(&mut self, security_id: i32) -> Result<()> {
        self.client
            .query(
                "SELECT * FROM daily_market_movements WHERE security_id = $1",
                &[&security_id],
            )
            .map(|_| ())
            .with_context(|| format!("failed to query daily_market_movements {security_id}"))
    }

    fn high_value_customers(&mut self) -> Result<()> {
        self.client
            .query("SELECT * FROM high_value_customers", &[])
            .map(|_| ())
            .with_context(|| "failed to query high_value_customers".to_string())
    }

    fn pending_orders_summary(&mut self, ticker: &str) -> Result<()> {
        self.client
            .query(
                "SELECT * FROM pending_orders_summary WHERE ticker = $1",
                &[&ticker],
            )
            .map(|_| ())
            .with_context(|| "failed to query pending_orders_summary".to_string())
    }

    fn trade_volume_by_hour(&mut self) -> Result<()> {
        self.client
            .query("SELECT * FROM trade_volume_by_hour", &[])
            .map(|_| ())
            .with_context(|| "failed to query trade_volume_by_hour".to_string())
    }

    fn top_securities_by_sector(&mut self, sector: String) -> Result<()> {
        self.client
            .query(
                "SELECT * FROM top_securities_by_sector WHERE sector = $1",
                &[&sector],
            )
            .map(|_| ())
            .with_context(|| "failed to query top_securities_by_sector".to_string())
    }

    fn recent_trades_by_account(&mut self, account_id: i32) -> Result<()> {
        self.client
            .query(
                "SELECT * FROM recent_trades_by_account WHERE account_id = $1",
                &[&account_id],
            )
            .map(|_| ())
            .with_context(|| format!("failed to query recent_trades_by_account {account_id}"))
    }

    fn order_fulfillment_rates(&mut self, customer_id: i32) -> Result<()> {
        self.client
            .query(
                "SELECT * FROM order_fulfillment_rates WHERE customer_id = $1",
                &[&customer_id],
            )
            .map(|_| ())
            .with_context(|| format!("failed to query order_fulfillment_rates {customer_id}"))
    }

    fn sector_order_activity(&mut self, sector: String) -> Result<()> {
        self.client
            .query(
                "SELECT * FROM sector_order_activity WHERE sector = $1",
                &[&sector],
            )
            .map(|_| ())
            .with_context(|| "failed to query sector_order_activity".to_string())
    }

    fn cascading_order_cancellation_alert(&mut self) -> Result<()> {
        self.client
            .query("SELECT * FROM cascading_order_cancellation_alert", &[])
            .map(|_| ())
            .with_context(|| "failed to query cascading_order_cancellation_alert".to_string())
    }
}
