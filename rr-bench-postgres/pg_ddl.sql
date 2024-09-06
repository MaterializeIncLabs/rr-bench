--- Table definitions for Postgres.

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
    ticker VARCHAR(10) NOT NULL,
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
