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


def run_pipeline_dq_checks(run_id):
    """
    Run comprehensive data quality checks for BD Transformer pipeline
    
    Args:
        run_id (str): Pipeline run ID to check
    """
    # Define DQ checks to run
    dq_checks = [
        ("Raw customer profiles data exists", "/opt/airflow/dq_checks/check_original_data_exists.sql"),
        ("Transformed customer profiles data exists", "/opt/airflow/dq_checks/check_transformed_data_exists.sql"),
        ("Inverted customer profiles data exists", "/opt/airflow/dq_checks/check_recovered_data_exists.sql"),
        ("Row count consistency across all tables", "/opt/airflow/dq_checks/check_row_count_consistency.sql"),
        ("No null values in critical columns", "/opt/airflow/dq_checks/check_no_null_values.sql")
    ]
    
    # Run all DQ checks
    for check_name, sql_file_path in dq_checks:
        run_dq_check(check_name, sql_file_path, run_id)
    
    print(f"All DQ checks passed for run_id: {run_id}")