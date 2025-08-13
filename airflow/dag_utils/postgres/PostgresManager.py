import pandas as pd
from sqlalchemy import create_engine, text
import os


class PostgresManager:
    """
    Centralized database connection and operations for Airflow pipelines
    
    Key Features:
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
    
    def ingest_from_parquet(self, parquet_path, table_name, ds, if_exists='overwrite', postgres_config=None):
        """
        Load Parquet data into PostgreSQL table using Spark JDBC (memory-efficient for large data)
        
        Args:
            parquet_path (str): Path to Parquet file
            table_name (str): Target PostgreSQL table name
            ds (str): Date string (YYYY-MM-DD format) from Airflow execution context
            if_exists (str): What to do if table exists ('overwrite', 'append', 'ignore', 'error', 'errorifexists', 'default')
        """
        from dag_utils.spark.SparkManager import SparkManager
        from pyspark.sql.functions import lit
        
        # Use passed postgres_config or fallback to environment variables
        if postgres_config is None:
            import os
            postgres_config = {
                'host': os.getenv('POSTGRES_HOST'),
                'port': os.getenv('POSTGRES_PORT'), 
                'database': os.getenv('POSTGRES_DB'),
                'user': os.getenv('POSTGRES_USER'),
                'password': os.getenv('POSTGRES_PASSWORD')
            }
        
        print(f"DEBUG - PostgresManager postgres_config: {postgres_config}")
        spark_manager = SparkManager("", {}, postgres_config)
        
        # Read parquet file
        df = spark_manager.read_from_parquet(parquet_path)
        
        # Add customer_id and ds columns using Spark (no memory conversion needed)
        from pyspark.sql.functions import monotonically_increasing_id, concat
        df_with_metadata = df \
            .withColumn('customer_id', concat(lit(ds), lit('_'), monotonically_increasing_id().cast('string'))) \
            .withColumn('ds', lit(ds))
        
        # Get row count before writing
        row_count = df_with_metadata.count()
        
        # Write directly to PostgreSQL using Spark JDBC (memory-efficient)
        spark_manager.write_to_postgres(df_with_metadata, table_name, mode=if_exists)
        
        print(f"Loaded {row_count} rows into {table_name} for ds={ds}")

    
    def run_dq_check(self, check_name, sql_query):
        """
        Run SQL DQ check with error handling
        """
        try:
            self.execute_sql(sql_query, f"DQ Check PASSED: {check_name}")
        except Exception as e:
            print(f"DQ Check FAILED: {check_name} - {str(e)}")
            raise e