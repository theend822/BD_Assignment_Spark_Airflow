-- BD Customer Profiles Inverted data table schema
-- Stores inverse-transformed data (should match original data format)

CREATE TABLE IF NOT EXISTS bd_customer_profiles_inverted (
    customer_id VARCHAR(255) PRIMARY KEY,
    
    -- Inverted customer profile columns (should match original schema after inverse transform)
    day_of_month INTEGER,
    height FLOAT,
    account_balance FLOAT,
    net_profit FLOAT,
    customer_ratings FLOAT,
    leaderboard_rank INTEGER,
    
    -- Metadata columns
    ds VARCHAR(10) NOT NULL
    
) PARTITION BY LIST (ds);