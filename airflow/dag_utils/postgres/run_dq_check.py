from dag_utils.postgres.PostgresManager import PostgresManager


def run_dq_check(check_name, sql_file_path, run_id):
    """
    Run data quality check using SQL file
    
    Args:
        check_name (str): Name/description of the DQ check
        sql_file_path (str): Path to SQL file containing the DQ check query
        run_id (str): Pipeline run ID to substitute in SQL
    """
    postgres_manager = PostgresManager()
    
    # Read SQL file and substitute run_id
    with open(sql_file_path, 'r') as f:
        sql_query = f.read().format(run_id=run_id)
    
    postgres_manager.run_dq_check(check_name, sql_query)


