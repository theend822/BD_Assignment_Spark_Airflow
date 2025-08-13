-- BD Customer Profiles Transformed data table schema  
-- Stores normalized/converted data after BD Transformer processing

CREATE TABLE IF NOT EXISTS bd_customer_profiles_transformed (
    id SERIAL PRIMARY KEY,
    run_id VARCHAR(255) NOT NULL,
    
    -- Transformed customer profile columns (normalized 0-1 values)
    day_of_month FLOAT,
    height FLOAT,
    account_balance FLOAT,
    net_profit FLOAT,
    customer_ratings FLOAT,
    leaderboard_rank FLOAT,
    
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create indices for better performance
CREATE INDEX IF NOT EXISTS idx_bd_customer_profiles_transformed_run_id ON bd_customer_profiles_transformed(run_id);
CREATE INDEX IF NOT EXISTS idx_bd_customer_profiles_transformed_created_at ON bd_customer_profiles_transformed(created_at);