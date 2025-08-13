from dag_utils.postgres.PostgresManager import PostgresManager


def ingest_data_from_parquet(parquet_path, table_name, run_id, if_exists='append'):
    """
    Load data from Parquet file to PostgreSQL table
    
    Args:
        parquet_path (str): Path to Parquet file
        table_name (str): Target PostgreSQL table name
        run_id (str): Unique identifier for this pipeline run
        if_exists (str): What to do if table exists ('append', 'replace', 'fail')
    """
    postgres_manager = PostgresManager()
    postgres_manager.ingest_from_parquet(parquet_path, table_name, run_id, if_exists)