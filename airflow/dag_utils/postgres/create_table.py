from dag_utils.postgres.PostgresManager import PostgresManager


def create_table(table_schema, log_message="Table created successfully"):
    """
    Create database table using SQL schema file
    
    Args:
        table_schema (str): Path to SQL file containing table creation statements
        log_message (str): Custom log message for successful table creation
    """
    postgres_manager = PostgresManager()
    
    # Read SQL schema file
    with open(table_schema, 'r') as f:
        sql_query = f.read()
    
    # Execute table creation SQL
    postgres_manager.execute_sql(sql_query, log_message)