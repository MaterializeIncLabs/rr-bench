use strum_macros::EnumIter;

pub enum WriteOperation {
    InsertCustomer {
        name: String,
        address: String,
    },
    InsertAccount {
        customer_id: i32,
        account_type: String,
        balance: f64,
        parent_account_id: Option<i32>,
    },
    InsertSecurity {
        ticker: String,
        name: String,
        sector: String,
    },
    InsertTrade {
        account_id: i32,
        security_id: i32,
        trade_type: String,
        quantity: i32,
        price: f64,
        parent_trade_id: Option<i32>,
    },
    InsertOrder {
        account_id: i32,
        security_id: i32,
        order_type: String,
        quantity: i32,
        limit_price: f64,
        status: String,
        parent_order_id: Option<i32>,
    },
    InsertMarketData {
        security_id: i32,
        price: f64,
        volume: i32,
    },
    UpdateCustomer {
        customer_id: i32,
        address: String,
    },
    UpdateAccount {
        account_id: i32,
        balance: f64,
    },
    UpdateTrade {
        trade_id: i32,
        price: f64,
    },
    UpdateOrder {
        order_id: i32,
        status: String,
        limit_price: f64,
    },
    UpdateMarketData {
        market_data_id: i32,
        price: f64,
        volume: i32,
    },

    DeleteCustomer {
        customer_id: i32,
    },
    DeleteAccount {
        account_id: i32,
    },
    DeleteSecurity {
        security_id: i32,
    },
    DeleteTrade {
        trade_id: i32,
    },
    DeleteOrder {
        order_id: i32,
    },
    DeleteMarketData {
        market_data_id: i32,
    },
}

#[derive(EnumIter, Debug, PartialEq)]
pub enum ReadOperation {
    CustomerPortfolio,
    TopPerformers,
    MarketOverview,
    RecentLargeTrades,
    CustomerOrderBook,
    SectorPerformance,
    AccountActivitySummary,
    DailyMarketMovements,
    HighValueCustomers,
    PendingOrdersSummary,
    TradeVolumeByHour,
    TopSecuritiesBySector,
    RecentTradesByAccount,
    OrderFulfillmentRates,
    SectorOrderActivity,
}
