# Read Replica Benchmark

### **1. Objective**

This benchmark evaluates the price-performance of read replicas in a database system, specifically under the workload of complex read queries that require fresh data. 
The benchmark is modeled after a brokerage system, where the primary database handles a continuous stream of transactional writes (inserts, updates, and deletes). At the same time, the read replicas are queried for various complex analytical operations.

## 1.1 Why a new benchmark

While a host of existing database benchmarks exist, they fall short of addressing the unique challenges presented by modern OLTP systems that rely heavily on read replicas and real-time analytics.

1. **TPC-C**:
    - TPC-C is focused on transaction processing, specifically measuring the throughput of simple OLTP transactions like order entry. While it effectively simulates high-volume transactional workloads, it doesn’t account for complex read queries or the performance of read replicas, which are critical in modern systems.
2. **TPC-E**:
    - TPC-E models a financial trading system, incorporating more complex transactions than TPC-C. However, it still primarily focuses on the performance of a single, primary database and doesn’t evaluate the challenges associated with maintaining data freshness and query performance on read replicas.
3. **TPC-H**:
    - TPC-H is designed to measure the performance of data warehousing systems through a set of analytical queries. While it does test complex queries, it is not designed for OLTP environments and doesn’t address the transactional aspects or the real-time nature of queries on read replicas.
4. **TPC-DS**:
    - TPC-DS extends the analytical focus of TPC-H with more complex queries and a broader range of data processing scenarios. However, like TPC-H, it is targeted at decision support systems and lacks the transactional focus needed for evaluating OLTP systems with read replicas.

### **System Under Test (SUT)**

- **Primary Database**: This database receives continuous write operations, simulating real-time trading activities such as order placements, trades, and market data updates.
- **Read Replica**: The read replica is used for executing complex SQL queries that involve multi-way joins, window functions, subqueries, aggregations, and recursive operations. The performance of these read replicas is critical, as they need to deliver up-to-date information with minimal replication lag.

### **2. Goals and Objectives**

The primary goal of this benchmark is to assess the effectiveness of read replicas in handling complex read operations in an OLTP environment. The benchmark will focus on three key areas:

1. **Query Performance**: Measure the latency and throughput of complex SQL queries that are typical in OLTP systems, such as those involving multi-way joins, window functions, and subqueries.
2. **Data Freshness**: Ensure that read replicas maintain minimal replication lag, so that the data served to users is as up-to-date as possible.
3. **System Scalability**: Evaluate how well the system scales as the number of read queries and write transactions increases.

### **3. Technical Details**

### **3.1. System Architecture**

The benchmark will simulate a typical OLTP system where:

- **Primary Database**: Handles all write transactions (inserts, updates, deletes) and replicates these changes to one or more read replicas.
- **Read Replicas**: Serve read queries, reducing the load on the primary database and providing scalability for read-heavy workloads.

### **3.2. Workload Simulation**

The benchmark workload is designed to simulate common OLTP patterns, including:

- **Inserts**: Adding new records, such as new customers, transactions, or orders. Represents around 45% of the traffic.
- **Updates**: Modifying existing records, such as updating account balances or order statuses. Represents around 45% of the traffic.
- **Deletes**: Removing records, though this operation will be less frequent to simulate real-world conditions where data is more often updated or read than deleted. Represents around 10% of the traffic.
- **Complex Reads**: Executing queries that involve multi-way joins, aggregations, and window functions to extract meaningful insights from the data.

The specific OLTP workload is modeled after a brokerage system, where the primary database continuously processes transactions like trade orders, customer account updates, and market data changes, while read replicas handle complex queries such as portfolio summaries, top-performing securities, and order fulfillment rates.

### **3.3. Benchmark Components**

1. **Primary Database Simulation**: A tool will simulate a continuous stream of write operations to the primary database, ensuring that it mimics the behavior of a busy OLTP system.
2. **Read Replica Querying**: The read replicas will be queried with a set of predefined complex SQL views. These views are designed to stress test the system by combining data from multiple tables and performing computationally intensive operations.
3. **Metrics Collection**: The benchmark will collect key performance metrics, including:
    - **Query Latency**: The time it takes for a query to execute on a read replica.
    - **Throughput**: The number of queries the read replicas can handle per second.
    

### **3.4. Implementation Considerations**

- **Data Model**: The data model is based on a brokerage system but can be generalized to other OLTP scenarios. Key tables include `customers`, `accounts`, `securities`, `trades`, `orders`, and `market_data`.
- **Scalability**: The system should be tested under varying loads, increasing the number of transactions and queries to understand how the read replicas scale.

### **4. Reference Implementation: PostgreSQL**

Below is a reference implementation of the benchmark using PostgreSQL. This includes table definitions and views to simulate the OLTP workload.

<aside>
💡 This workload is intended to highlight the SQL users would want to execute against a read replica. It may or may not be a workload where Materialize is the strongest today.

</aside>

### **4.1. Table Definitions**

```sql
CREATE TABLE customers (
    customer_id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    address VARCHAR(255),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE accounts (
    account_id SERIAL PRIMARY KEY,
    customer_id INT REFERENCES customers(customer_id) ON DELETE CASCADE,
    account_type VARCHAR(50) NOT NULL,
    balance DECIMAL(18, 2) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE securities (
    security_id SERIAL PRIMARY KEY,
    ticker VARCHAR(10) NOT NULL UNIQUE,
    name VARCHAR(255),
    sector VARCHAR(50),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE trades (
    trade_id SERIAL PRIMARY KEY,
    account_id INT REFERENCES accounts(account_id) ON DELETE CASCADE,
    security_id INT REFERENCES securities(security_id) ON DELETE CASCADE,
    trade_type VARCHAR(10) NOT NULL CHECK (trade_type IN ('buy', 'sell')),
    quantity INT NOT NULL,
    price DECIMAL(18, 4) NOT NULL,
    trade_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE orders (
    order_id SERIAL PRIMARY KEY,
    account_id INT REFERENCES accounts(account_id) ON DELETE CASCADE,
    security_id INT REFERENCES securities(security_id) ON DELETE CASCADE,
    order_type VARCHAR(10) NOT NULL CHECK (order_type IN ('buy', 'sell')),
    quantity INT NOT NULL,
    limit_price DECIMAL(18, 4),
    status VARCHAR(10) NOT NULL CHECK (status IN ('pending', 'completed', 'canceled')),
    order_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE market_data (
    market_data_id SERIAL PRIMARY KEY,
    security_id INT REFERENCES securities(security_id) ON DELETE CASCADE,
    price DECIMAL(18, 4) NOT NULL,
    volume INT NOT NULL,
    market_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

```

### **4.2. Views for Benchmarking**

1. **Customer Portfolio View**

    ```sql
    -- queries read for a single customer profile
    CREATE VIEW customer_portfolio AS
    SELECT c.customer_id, c.name, a.account_id, s.ticker, s.name AS security_name,
           SUM(t.quantity * t.price) AS total_value
    FROM customers c
    JOIN accounts a ON c.customer_id = a.customer_id
    JOIN trades t ON a.account_id = t.account_id
    JOIN securities s ON t.security_id = s.security_id
    GROUP BY c.customer_id, c.name, a.account_id, s.ticker, s.name;
    ```

2. **Top Performers View**
    
    ```sql
    -- queries return the full set of top performers
    CREATE VIEW top_performers AS
    WITH trade_volume AS (
        SELECT security_id, SUM(quantity) AS total_traded_volume
        FROM trades
        GROUP BY security_id
        ORDER BY SUM(quantity) DESC
        LIMIT 10
    )
    
    SELECT s.ticker, s.name, t.total_traded_volume
    FROM trade_volume t
    JOIN securities s USING (security_id);
    
    ```
    
3. **Market Overview View**
    
    ```sql
    CREATE VIEW market_overview AS
    SELECT s.sector, AVG(md.price) AS avg_price, SUM(md.volume) AS total_volume,
           MAX(md.market_date) AS last_update
    FROM securities s
    LEFT JOIN market_data md ON s.security_id = md.security_id
    GROUP BY s.sector
    HAVING MAX(md.market_date) > NOW() - INTERVAL '5 minutes';
    
    ```
    
4. **Recent Large Trades View**
    
    ```sql
    -- queries search for large trades by account_id
    CREATE VIEW recent_large_trades AS
    SELECT t.trade_id, a.account_id, s.ticker, t.quantity, t.price, t.trade_date
    FROM trades t
    JOIN accounts a ON t.account_id = a.account_id
    JOIN securities s ON t.security_id = s.security_id
    WHERE t.quantity > (SELECT AVG(quantity) FROM trades) * 5
      AND t.trade_date + INTERVAL '1 hour' > now();
    ```
    
5. **Customer Order Book**
    
    ```sql
    -- queries search for orders books by customer_id
    CREATE VIEW customer_order_book AS
    SELECT c.customer_id, c.name, COUNT(o.order_id) AS open_orders,
           SUM(CASE WHEN o.status = 'completed' THEN 1 ELSE 0 END) AS completed_orders
    FROM customers c
    JOIN accounts a ON c.customer_id = a.customer_id
    JOIN orders o ON a.account_id = o.account_id
    GROUP BY c.customer_id, c.name;
    
    ```
    
6. **Sector Performance**
    
    ```sql
    CREATE VIEW sector_performance AS
    SELECT s.sector, AVG(t.price) AS avg_trade_price, COUNT(t.trade_id) AS trade_count,
           SUM(t.quantity) AS total_volume
    FROM trades t
    JOIN securities s ON t.security_id = s.security_id
    GROUP BY s.sector;
    
    ```
    
7. **Account Activity Summary**
    
    ```sql
    -- queries select rows byased on account_id
    CREATE VIEW account_activity_summary AS
    SELECT a.account_id, COUNT(t.trade_id) AS trade_count,
           SUM(t.quantity * t.price) AS total_trade_value,
           MAX(t.trade_date) AS last_trade_date
    FROM accounts a
    LEFT JOIN trades t ON a.account_id = t.account_id
    GROUP BY a.account_id;
    ```
    
8. **Sector Performance**
    
    ```sql
    -- queries select rows based on sector
    CREATE VIEW sector_performance AS
    SELECT s.sector, AVG(t.price) AS avg_trade_price, COUNT(t.trade_id) AS trade_count,
           SUM(t.quantity) AS total_volume
    FROM trades t
    JOIN securities s ON t.security_id = s.security_id
    GROUP BY s.sector;
    ```
    
9. **High-Value Customers**
    
    ```sql
    -- returns all rows
    CREATE VIEW high_value_customers AS
    SELECT c.customer_id, c.name, SUM(a.balance) AS total_balance
    FROM customers c
    JOIN accounts a ON c.customer_id = a.customer_id
    GROUP BY c.customer_id, c.name
    HAVING SUM(a.balance) > 1000000;
    ```
    
10. **Pending Orders Summary**
    
    ```sql
    -- queries result based on ticker
    CREATE VIEW pending_orders_summary AS
    SELECT s.ticker, s.name, COUNT(o.order_id) AS pending_order_count,
           SUM(o.quantity) AS pending_volume,
           AVG(o.limit_price) AS avg_limit_price
    FROM orders o
    JOIN securities s ON o.security_id = s.security_id
    WHERE o.status = 'pending'
    GROUP BY s.ticker, s.name;
    ```
    
11. **Trade Volume by Hour**
    
    ```sql
    -- queries return all rows
    CREATE VIEW trade_volume_by_hour AS
    SELECT EXTRACT(HOUR FROM t.trade_date) AS trade_hour,
           COUNT(t.trade_id) AS trade_count,
           SUM(t.quantity) AS total_quantity
    FROM trades t
    GROUP BY EXTRACT(HOUR FROM t.trade_date);
    ```
    
12. **Top Securities by Sector**
    
    ```sql
    -- queries rows based on sector
    CREATE VIEW top_securities_by_sector AS
    SELECT grp.sector, ticker, name, total_volume
    FROM (SELECT DISTINCT sector FROM securities) grp,
        LATERAL (
            SELECT s.sector, s.ticker, s.name, SUM(t.quantity) AS total_volume
            FROM trades t
                     JOIN securities s ON t.security_id = s.security_id
            WHERE s.sector = grp.sector
            GROUP BY s.sector, s.ticker, s.name
            ORDER BY total_volume DESC
            LIMIT 5
    );
    ```
    
13. **Recent Trades by Account**
    
    ```sql
    -- queries rows based on account id
    CREATE VIEW recent_trades_by_account AS
    SELECT a.account_id, s.ticker, t.quantity, t.price, t.trade_date
    FROM trades t
    JOIN accounts a ON t.account_id = a.account_id
    JOIN securities s ON t.security_id = s.security_id
    WHERE t.trade_date + INTERVAL '1 day'> now();
    ```
    
14. **Order Fulfillment Rates**
    
    ```sql
    -- queries rows based on customer_id
    CREATE VIEW order_fulfillment_rates AS
    SELECT c.customer_id, c.name,
           COUNT(o.order_id) AS total_orders,
           SUM(CASE WHEN o.status = 'completed' THEN 1 ELSE 0 END) AS fulfilled_orders,
           (SUM(CASE WHEN o.status = 'completed' THEN 1 ELSE 0 END) * 100.0 / COUNT(o.order_id)) AS fulfillment_rate
    FROM customers c
    JOIN accounts a ON c.customer_id = a.customer_id
    JOIN orders o ON a.account_id = o.account_id
    GROUP BY c.customer_id, c.name;
    ```
    
15. **Sector-wise Order Activity**
    
    ```sql
    -- queries rows based on sector
    CREATE VIEW sector_order_activity AS
    SELECT s.sector, COUNT(o.order_id) AS order_count,
           SUM(o.quantity) AS total_quantity,
           AVG(o.limit_price) AS avg_limit_price
    FROM orders o
    JOIN securities s ON o.security_id = s.security_id
    GROUP BY s.sector;
    ```
    

16. **Daily Market Movements**

    ```sql
    -- queries rows based on security_id
    CREATE VIEW daily_market_movements AS
    WITH last_two_days AS (
        SELECT grp.security_id, price, market_date
        FROM (SELECT DISTINCT security_id FROM market_data) grp,
            LATERAL (
                SELECT md.security_id, md.price, md.market_date
                FROM market_data md
                WHERE md.security_id = grp.security_id AND md.market_date + INTERVAL '1 day' > now()
                ORDER BY md.market_date DESC
                LIMIT 2
            )
    ),
    
    stg AS (
        SELECT security_id, today.price AS current_price, yesterday.price AS previous_price, today.market_date
        FROM last_two_days today
        LEFT JOIN last_two_days yesterday USING (security_id)
        WHERE today.market_date > yesterday.market_date
    )
    
    SELECT
        security_id,
        ticker,
        name,
        current_price,
        previous_price,
        current_price - previous_price AS price_change,
        market_date
    FROM stg
    JOIN securities USING (security_id);
    ```