-- DQ Check: Verify no critical null values in raw customer profiles data
-- This should return 1 if no null values in key columns

SELECT CASE 
    WHEN (
        SELECT COUNT(*) 
        FROM bd_customer_profiles_raw 
        WHERE run_id = '{run_id}' 
        AND (day_of_month IS NULL OR height IS NULL OR account_balance IS NULL)
    ) = 0 THEN 1 
    ELSE 1/0  -- This will cause error if null values found
END as null_check;