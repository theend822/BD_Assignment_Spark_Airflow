-- DQ Check: Verify all customer profile tables have same row count for run_id
-- This should return 1 if all tables have same number of rows

SELECT CASE 
    WHEN (
        (SELECT COUNT(*) FROM bd_customer_profiles_raw WHERE run_id = '{run_id}') =
        (SELECT COUNT(*) FROM bd_customer_profiles_transformed WHERE run_id = '{run_id}') AND
        (SELECT COUNT(*) FROM bd_customer_profiles_transformed WHERE run_id = '{run_id}') =
        (SELECT COUNT(*) FROM bd_customer_profiles_inverted WHERE run_id = '{run_id}')
    ) THEN 1 
    ELSE 1/0  -- This will cause error if counts don't match
END as consistency_check;