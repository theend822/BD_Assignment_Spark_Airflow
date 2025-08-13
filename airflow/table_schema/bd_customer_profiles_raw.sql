-- BD Customer Profiles Raw data table schema
-- Stores raw input data as loaded from parquet files

CREATE TABLE IF NOT EXISTS bd_customer_profiles_raw (
    id SERIAL PRIMARY KEY,
    run_id VARCHAR(255) NOT NULL,
    
    -- Raw customer profile columns (same as input parquet schema)
    day_of_month INTEGER,
    height FLOAT,
    account_balance FLOAT,
    net_profit FLOAT,
    customer_ratings FLOAT,
    leaderboard_rank INTEGER,
    
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create indices for better performance
CREATE INDEX IF NOT EXISTS idx_bd_customer_profiles_raw_run_id ON bd_customer_profiles_raw(run_id);
CREATE INDEX IF NOT EXISTS idx_bd_customer_profiles_raw_created_at ON bd_customer_profiles_raw(created_at);