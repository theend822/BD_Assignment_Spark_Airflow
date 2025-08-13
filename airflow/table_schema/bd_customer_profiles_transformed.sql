-- BD Customer Profiles Transformed data table schema  
-- Stores normalized/converted data after BD Transformer processing

CREATE TABLE IF NOT EXISTS bd_customer_profiles_transformed (
    customer_id VARCHAR(255) PRIMARY KEY,
    
    -- Transformed customer profile columns (normalized 0-1 values)
    day_of_month FLOAT,
    height FLOAT,
    account_balance FLOAT,
    net_profit FLOAT,
    customer_ratings FLOAT,
    leaderboard_rank FLOAT,
    
    -- Metadata columns
    ds VARCHAR(10) NOT NULL
    
) PARTITION BY LIST (ds);
