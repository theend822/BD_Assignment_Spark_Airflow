# DQ Check: Verify if there are NULL values in key columns of any table
# This should return 1 if no null values in key columns

def generate_null_dq_check_sql(col_list, ds, source_table):
    """
    Generate SQL for DQ check on null values in key columns
    """
    return f"""
        WITH null_cnt as (
            SELECT
                count(*) as null_count
            FROM {source_table} 
            WHERE ds = '{ds}'
            and (
                {' or '.join(f"{col} is null" for col in col_list)}
            )
        )
        SELECT
            CASE WHEN null_count>0 THEN 1/0 ELSE 1 END as null_check
        FROM null_cnt
        ;
    """


