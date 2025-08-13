import pandas as pd
from sqlalchemy import create_engine, text
import os


class PostgresManager:
    """
    Centralized database connection and operations for BD Transformer pipeline
    
    Purpose:
    - Avoid repetitive database connection code across multiple Airflow tasks
    - Leverage environment variables for connection configuration
    - Provides consistent error handling and logging for database operations
    """
    
    def __init__(self):
        self.conn_string = os.getenv("AIRFLOW__DATABASE__SQL_ALCHEMY_CONN")
        if not self.conn_string:
            raise ValueError("AIRFLOW__DATABASE__SQL_ALCHEMY_CONN environment variable not set")
        self.engine = create_engine(self.conn_string)
    
    def execute_sql(self, sql_query, log_message="SQL executed successfully"):
        """
        Execute any SQL query with custom logging
        
        Returns: SQLAlchemy result object (for potential debugging/inspection)
        """
        with self.engine.begin() as conn:
            conn.execute(text(sql_query))
            print(log_message)
    
    def ingest_from_parquet(self, parquet_path, table_name, run_id, if_exists='append'):
        """
        Load Parquet data into PostgreSQL table using shared Spark session
        """
        from dag_utils.spark.SparkManager import SparkManager
        
        # Use shared Spark session
        spark_manager = SparkManager()
        
        # Read parquet file
        df = spark_manager.read_parquet(parquet_path)
        
        # Convert to Pandas and add run_id
        pandas_df = spark_manager.convert_to_pandas(df)
        pandas_df['run_id'] = run_id
        
        # Save to PostgreSQL
        pandas_df.to_sql(
            table_name,
            self.engine,
            if_exists=if_exists,
            index=False,
            method='multi'
        )
        
        print(f"Loaded {len(pandas_df)} rows into {table_name}")

    
    def run_dq_check(self, check_name, sql_query):
        """
        Run SQL DQ check with error handling
        """
        try:
            self.execute_sql(sql_query, f"DQ Check PASSED: {check_name}")
        except Exception as e:
            print(f"DQ Check FAILED: {check_name} - {str(e)}")
            raise e