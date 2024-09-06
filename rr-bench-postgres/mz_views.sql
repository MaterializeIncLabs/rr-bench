-- View definitions for the queries that will
-- be executed against the Materialize instance when under
-- test. The only difference between these queries and
-- the ones defined in pg_views.sql is the use of
-- mz_now() and the view indexes. 

CREATE VIEW customer_portfolio AS
SELECT c.customer_id, c.name, a.account_id, s.ticker, s.name AS security_name,
       SUM(t.quantity * t.price) AS total_value
FROM customers c
JOIN accounts a ON c.customer_id = a.customer_id
JOIN trades t ON a.account_id = t.account_id
JOIN securities s ON t.security_id = s.security_id
GROUP BY c.customer_id, c.name, a.account_id, s.ticker, s.name;

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

CREATE VIEW market_overview AS
SELECT s.sector, AVG(md.price) AS avg_price, SUM(md.volume) AS total_volume,
       MAX(md.market_date) AS last_update
FROM securities s
LEFT JOIN market_data md ON s.security_id = md.security_id
GROUP BY s.sector
HAVING MAX(md.market_date) + INTERVAL '5 minutes' > mz_now() ;

CREATE VIEW recent_large_trades AS
SELECT t.trade_id, a.account_id, s.ticker, t.quantity, t.price, t.trade_date
FROM trades t
JOIN accounts a ON t.account_id = a.account_id
JOIN securities s ON t.security_id = s.security_id
WHERE t.quantity > (SELECT AVG(quantity) FROM trades) * 5
  AND t.trade_date + INTERVAL '1 hour' > mz_now();


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
WITH last_two_days AS (
    SELECT grp.security_id, price, market_date
    FROM (SELECT DISTINCT security_id FROM market_data) grp,
    LATERAL (
        SELECT md.security_id, md.price, md.market_date
        FROM market_data md
        WHERE md.security_id = grp.security_id AND md.market_date + INTERVAL '1 day' > mz_now()
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
SELECT EXTRACT(HOUR FROM t.trade_date) AS trade_hour,
       COUNT(t.trade_id) AS trade_count,
       SUM(t.quantity) AS total_quantity
FROM trades t
GROUP BY EXTRACT(HOUR FROM t.trade_date);

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


CREATE VIEW recent_trades_by_account AS
SELECT a.account_id, s.ticker, t.quantity, t.price, t.trade_date
FROM trades t
JOIN accounts a ON t.account_id = a.account_id
JOIN securities s ON t.security_id = s.security_id
WHERE t.trade_date + INTERVAL '1 day'> mz_now() ;


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

CREATE INDEX ON securities (security_id);
CREATE INDEX ON accounts (account_id);
CREATE INDEX ON customers (customer_id);
CREATE INDEX ON customer_portfolio (customer_id);
CREATE INDEX ON top_performers (ticker);
CREATE INDEX ON market_overview (sector);
CREATE INDEX ON recent_large_trades (trade_id);
CREATE INDEX ON customer_order_book (customer_id);
CREATE INDEX ON account_activity_summary (account_id);
CREATE INDEX ON daily_market_movements (security_id); -- hi
CREATE INDEX ON high_value_customers (customer_id);
CREATE INDEX ON pending_orders_summary (ticker);
CREATE INDEX ON trade_volume_by_hour (trade_hour);
CREATE INDEX ON top_securities_by_sector (sector);
CREATE INDEX ON recent_trades_by_account (account_id);
CREATE INDEX ON order_fulfillment_rates (customer_id);
CREATE INDEX ON sector_order_activity (sector);
CREATE INDEX ON sector_performance (sector);
