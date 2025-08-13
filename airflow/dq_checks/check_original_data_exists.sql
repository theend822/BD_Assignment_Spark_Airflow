-- DQ Check: Verify raw customer profiles data exists for run_id
-- This should return at least 1 row if data exists

SELECT 1 
FROM bd_customer_profiles_raw 
WHERE run_id = '{run_id}' 
LIMIT 1;