# DQ Check: Verify all customer profile tables have same row count for ds
# This should return 1 if all tables have same number of rows

def generate_row_cnt_consistency_dq_check_sql(table_list, ds):
    """
    Generate SQL for DQ check on row count consistency across all customer profile tables
    """
    return f"""
        SELECT CASE 
            WHEN (
                {' AND '.join(f"(SELECT COUNT(*) FROM {table_list[i]} WHERE ds = '{ds}') = (SELECT COUNT(*) FROM {table_list[i+1]} WHERE ds = '{ds}')" for i in range(len(table_list)-1))}
            ) THEN 1 
            ELSE 1/0
        END as consistency_check;
    """

