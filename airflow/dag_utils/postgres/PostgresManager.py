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
    
    def ingest_from_parquet(self, parquet_path, table_name, run_id, ds, if_exists='append'):
        """
        Load Parquet data into PostgreSQL table using Spark JDBC (memory-efficient for large data)
        
        Args:
            parquet_path (str): Path to Parquet file
            table_name (str): Target PostgreSQL table name
            run_id (str): Unique identifier for this pipeline run
            ds (str): Date string (YYYY-MM-DD format) for partitioning
            if_exists (str): What to do if table exists ('append', 'replace', 'fail')
        """
        from dag_utils.spark.SparkManager import SparkManager
        from pyspark.sql.functions import lit
        
        # Use shared Spark session
        spark_manager = SparkManager()
        
        # Read parquet file
        df = spark_manager.read_parquet(parquet_path)
        
        # Add customer_id, ds and run_id columns using Spark (no memory conversion needed)
        from pyspark.sql.functions import monotonically_increasing_id, concat
        df_with_metadata = df \
            .withColumn('customer_id', concat(lit(ds), lit('_'), monotonically_increasing_id().cast('string'))) \
            .withColumn('ds', lit(ds)) \
            .withColumn('run_id', lit(run_id))
        
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