from dag_utils.postgres.PostgresManager import PostgresManager


def ingest_data_from_parquet(parquet_path, table_name, ds, if_exists='overwrite', postgres_config=None):
    """
    Load data from Parquet file to PostgreSQL table
    
    Args:
        parquet_path (str): Path to Parquet file
        table_name (str): Target PostgreSQL table name
        ds (str): Date string (YYYY-MM-DD format) from Airflow execution context
        if_exists (str): What to do if table exists ('overwrite', 'append', 'ignore', 'error', 'errorifexists', 'default'
        postgres_config (dict): PostgreSQL connection configuration
    """
    postgres_manager = PostgresManager()
    postgres_manager.ingest_from_parquet(parquet_path, table_name, ds, if_exists, postgres_config)