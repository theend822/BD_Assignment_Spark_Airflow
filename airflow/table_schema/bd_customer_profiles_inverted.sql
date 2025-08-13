-- BD Customer Profiles Inverted data table schema
-- Stores inverse-transformed data (should match original data format)

CREATE TABLE IF NOT EXISTS bd_customer_profiles_inverted (
    id SERIAL PRIMARY KEY,
    run_id VARCHAR(255) NOT NULL,
    
    -- Inverted customer profile columns (should match original schema after inverse transform)
    day_of_month INTEGER,
    height FLOAT,
    account_balance FLOAT,
    net_profit FLOAT,
    customer_ratings FLOAT,
    leaderboard_rank INTEGER,
    
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create indices for better performance
CREATE INDEX IF NOT EXISTS idx_bd_customer_profiles_inverted_run_id ON bd_customer_profiles_inverted(run_id);
CREATE INDEX IF NOT EXISTS idx_bd_customer_profiles_inverted_created_at ON bd_customer_profiles_inverted(created_at);