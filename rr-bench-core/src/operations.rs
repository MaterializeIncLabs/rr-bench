pub enum Operation {
    InsertCustomer {
        name: String,
        address: String,
    },
    InsertAccount {
        customer_id: u64,
        account_type: String,
        balance: f64,
        parent_account_id: Option<u64>,
    },
    InsertSecurity {
        ticker: String,
        name: String,
        sector: String,
    },
    InsertTrade {
        account_id: u64,
        security_id: u64,
        trade_type: String,
        quantity: i32,
        price: f64,
        parent_trade_id: Option<u64>,
    },
    InsertOrder {
        account_id: u64,
        security_id: u64,
        order_type: String,
        quantity: i32,
        limit_price: f64,
        status: String,
        parent_order_id: Option<u64>,
    },
    InsertMarketData {
        security_id: u64,
        price: f64,
        volume: i32,
    },

    UpdateCustomer {
        customer_id: u64,
        address: String,
    },
    UpdateAccount {
        account_id: u64,
        balance: f64,
    },
    UpdateTrade {
        trade_id: u64,
        price: f64,
    },
    UpdateOrder {
        order_id: u64,
        status: String,
        limit_price: f64,
    },
    UpdateMarketData {
        market_data_id: u64,
        price: f64,
        volume: f64,
    },

    DeleteCustomer {
        customer_id: u64,
    },
    DeleteAccount {
        account_id: u64,
    },
    DeleteSecurity {
        security_id: u64,
    },
    DeleteTrade {
        trade_id: u64,
    },
    DeleteOrder {
        order_id: u64,
    },
    DeleteMarketData {
        market_data_id: u64,
    },
}
