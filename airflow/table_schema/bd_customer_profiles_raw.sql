-- BD Customer Profiles Raw data table schema
-- Stores raw input data as loaded from parquet files

CREATE TABLE IF NOT EXISTS bd_customer_profiles_raw (
    customer_id VARCHAR(255) NOT NULL,
    
    -- Raw customer profile columns (same as input parquet schema)
    day_of_month INTEGER,
    height FLOAT,
    account_balance FLOAT,
    net_profit FLOAT,
    customer_ratings FLOAT,
    leaderboard_rank INTEGER,
    
    -- Metadata columns
    ds VARCHAR(10) NOT NULL,
    
    -- Composite primary key including partition column
    PRIMARY KEY (customer_id, ds)
) PARTITION BY LIST (ds);
