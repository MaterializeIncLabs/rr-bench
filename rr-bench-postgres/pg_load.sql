-- Utilities for loading data generated
-- by rr-data-gen binary.

-- Load data into customers table
\copy customers (customer_id, name, address, created_at)
FROM 'data/customers.csv'
DELIMITER ','
CSV HEADER;

-- Load data into accounts table
\copy accounts (account_id, customer_id, account_type, balance, created_at)
FROM 'data/accounts.csv'
DELIMITER ','
CSV HEADER;

-- Load data into securities table
\copy securities (security_id, ticker, name, sector, created_at)
FROM 'data/securities.csv'
DELIMITER ','
CSV HEADER;

-- Load data into trades table
\copy trades (trade_id, account_id, security_id, trade_type, quantity, price, trade_date)
FROM 'data/trades.csv'
DELIMITER ','
CSV HEADER;

-- Load data into orders table
\copy orders (order_id, account_id, security_id, order_type, quantity, limit_price, status, order_date)
FROM 'data/orders.csv'
DELIMITER ','
CSV HEADER;

-- Load data into market_data table
\copy market_data (market_data_id, security_id, price, volume, market_date)
FROM 'data/market_data.csv'
DELIMITER ','
CSV HEADER;

select setval('customers_customer_id_seq', (select max(customer_id) from customers) + 1);
select setval('accounts_account_id_seq', (select max(account_id) from accounts) + 1);
select setval('securities_security_id_seq', (select max(security_id) from securities) + 1);
select setval('trades_trade_id_seq', (select max(trade_id) from trades) + 1);
select setval('orders_order_id_seq', (select max(order_id) from orders) + 1);
select setval('market_data_market_data_id_seq', (select max(market_data_id) from market_data) + 1);
