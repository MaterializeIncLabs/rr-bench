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

CREATE TABLE IF NOT EXISTS securities (
    security_id INTEGER PRIMARY KEY AUTOINCREMENT,
    ticker TEXT NOT NULL,
    name TEXT,
    sector TEXT,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
);

CREATE VIEW customer_portfolio AS
SELECT c.customer_id, c.name, a.account_id, s.ticker, s.name AS security_name,
       SUM(t.quantity * t.price) AS total_value
FROM customers c
JOIN accounts a ON c.customer_id = a.customer_id
JOIN trades t ON a.account_id = t.account_id
JOIN securities s ON t.security_id = s.security_id
GROUP BY c.customer_id, c.name, a.account_id, s.ticker, s.name;

CREATE VIEW top_performers AS
WITH ranked_performers AS (
    SELECT s.ticker, s.name, SUM(t.quantity) AS total_traded_volume,
           ROW_NUMBER() OVER (ORDER BY SUM(t.quantity) DESC) AS rank
    FROM trades t
    JOIN securities s ON t.security_id = s.security_id
    GROUP BY s.ticker, s.name
)
SELECT ticker, name, total_traded_volume, rank
FROM ranked_performers
WHERE rank <= 10;

CREATE VIEW market_overview AS
SELECT s.sector, 
       AVG(md.price) AS avg_price, 
       SUM(md.volume) AS total_volume,
       MAX(md.market_date) AS last_update
FROM securities s
LEFT JOIN market_data md ON s.security_id = md.security_id
GROUP BY s.sector
HAVING MAX(md.market_date) > datetime('now', '-5 minutes');

CREATE VIEW recent_large_trades AS
SELECT t.trade_id, a.account_id, s.ticker, t.quantity, t.price, t.trade_date
FROM trades t
JOIN accounts a ON t.account_id = a.account_id
JOIN securities s ON t.security_id = s.security_id
WHERE t.quantity > (SELECT AVG(quantity) FROM trades) * 5
AND t.trade_date > datetime('now', '-1 hour');

CREATE VIEW customer_order_book AS
SELECT c.customer_id, c.name, COUNT(o.order_id) AS open_orders,
       SUM(CASE WHEN o.status = 'completed' THEN 1 ELSE 0 END) AS completed_orders
FROM customers c
JOIN accounts a ON c.customer_id = a.customer_id
JOIN orders o ON a.account_id = o.account_id
GROUP BY c.customer_id, c.name;

CREATE VIEW sector_performance AS
SELECT s.sector, AVG(t.price) AS avg_trade_price, COUNT(t.trade_id) AS trade_count,
       SUM(t.quantity) AS total_volume
FROM trades t
JOIN securities s ON t.security_id = s.security_id
GROUP BY s.sector;

CREATE VIEW account_activity_summary AS
SELECT a.account_id, COUNT(t.trade_id) AS trade_count, 
       SUM(t.quantity * t.price) AS total_trade_value,
       MAX(t.trade_date) AS last_trade_date
FROM accounts a
LEFT JOIN trades t ON a.account_id = t.account_id
GROUP BY a.account_id;

CREATE VIEW daily_market_movements AS
SELECT md.security_id, s.ticker, s.name,
       md.price AS current_price,
       LAG(md.price) OVER (PARTITION BY md.security_id ORDER BY md.market_date) AS previous_price,
       (md.price - LAG(md.price) OVER (PARTITION BY md.security_id ORDER BY md.market_date)) AS price_change,
       md.market_date
FROM market_data md
JOIN securities s ON md.security_id = s.security_id
WHERE md.market_date > datetime('now', '-1 day');

CREATE VIEW high_value_customers AS
SELECT c.customer_id, c.name, SUM(a.balance) AS total_balance
FROM customers c
JOIN accounts a ON c.customer_id = a.customer_id
GROUP BY c.customer_id, c.name
HAVING SUM(a.balance) > 1000000;

CREATE VIEW pending_orders_summary AS
SELECT s.ticker, s.name, COUNT(o.order_id) AS pending_order_count,
       SUM(o.quantity) AS pending_volume,
       AVG(o.limit_price) AS avg_limit_price
FROM orders o
JOIN securities s ON o.security_id = s.security_id
WHERE o.status = 'pending'
GROUP BY s.ticker, s.name;

CREATE VIEW trade_volume_by_hour AS
SELECT strftime('%H', t.trade_date) AS trade_hour,
       COUNT(t.trade_id) AS trade_count,
       SUM(t.quantity) AS total_quantity
FROM trades t
GROUP BY strftime('%H', t.trade_date);

CREATE VIEW top_securities_by_sector AS
WITH ranked_securities AS (
    SELECT s.sector, s.ticker, s.name,
           SUM(t.quantity) AS total_volume,
           ROW_NUMBER() OVER (PARTITION BY s.sector ORDER BY SUM(t.quantity) DESC) AS sector_rank
    FROM trades t
    JOIN securities s ON t.security_id = s.security_id
    GROUP BY s.sector, s.ticker, s.name
)
SELECT sector, ticker, name, total_volume, sector_rank
FROM ranked_securities
WHERE sector_rank <= 5;

CREATE VIEW recent_trades_by_account AS
SELECT a.account_id, s.ticker, t.quantity, t.price, t.trade_date
FROM trades t
JOIN accounts a ON t.account_id = a.account_id
JOIN securities s ON t.security_id = s.security_id
WHERE t.trade_date > datetime('now', '-1 day');

CREATE VIEW order_fulfillment_rates AS
SELECT c.customer_id, c.name,
       COUNT(o.order_id) AS total_orders,
       SUM(CASE WHEN o.status = 'completed' THEN 1 ELSE 0 END) AS fulfilled_orders,
       (SUM(CASE WHEN o.status = 'completed' THEN 1 ELSE 0 END) * 100.0 / COUNT(o.order_id)) AS fulfillment_rate
FROM customers c
JOIN accounts a ON c.customer_id = a.customer_id
JOIN orders o ON a.account_id = o.account_id
GROUP BY c.customer_id, c.name;

CREATE VIEW sector_order_activity AS
SELECT s.sector, COUNT(o.order_id) AS order_count,
       SUM(o.quantity) AS total_quantity,
       AVG(o.limit_price) AS avg_limit_price
FROM orders o
JOIN securities s ON o.security_id = s.security_id
GROUP BY s.sector;

CREATE VIEW cascading_order_cancellation_alert AS
WITH RECURSIVE order_cancellations AS (
    SELECT
        o.order_id,
        o.account_id,
        o.security_id,
        o.status,
        o.order_date,
        NULL AS parent_order_id,
        0 AS cancellation_depth
    FROM orders o
    WHERE o.status = 'canceled'
    AND o.order_date = (
        SELECT MAX(o2.order_date)
        FROM orders o2
        WHERE o.security_id = o2.security_id
    )

    UNION ALL

    SELECT
        o.order_id,
        o.account_id,
        o.security_id,
        o.status,
        o.order_date,
        oc.order_id AS parent_order_id,
        oc.cancellation_depth + 1 AS cancellation_depth
    FROM orders o
    JOIN order_cancellations oc
    ON o.security_id = oc.security_id
    AND o.status = 'canceled'
    AND o.order_date > oc.order_date
)
SELECT *
FROM order_cancellations
WHERE cancellation_depth > 0;