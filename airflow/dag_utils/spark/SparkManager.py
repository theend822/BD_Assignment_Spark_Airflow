import os
from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from typing import Optional
import atexit


class SparkManager:
    """
    Centralized Spark session management for Airflow pipeline
    
    Key Features:
    - One Spark session per DAG worker (allows concurrent DAGs)
    - Centralized configuration and connection management
    - PostgreSQL JDBC integration for reading database tables
    - Consistent error handling across all Spark operations
    """
    
    def __init__(self):
        """Initialize SparkManager for each DAG worker"""
        self._spark_session = None
        # Register cleanup on exit
        atexit.register(self.stop_spark_session)
    
    def start_spark_session(self, app_name: str):
        """
        Start Spark session with optimized configuration
        
        Args:
            app_name (str): Name for the Spark application
        """
        if self._spark_session is not None:
            print(f"Spark session already running: {self._spark_session.sparkContext.appName}")
            return
        
        try:
            print(f"Starting Spark session: {app_name}")
            
            self._spark_session = SparkSession.builder \
                .appName(app_name) \
                .config("spark.driver.memory", "2g") \
                .config("spark.executor.memory", "2g") \
                .config("spark.driver.maxResultSize", "1g") \
                .config("spark.sql.adaptive.enabled", "true") \
                .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
                .config("spark.jars", "/opt/spark/jars/postgresql-42.6.0.jar") \
                .getOrCreate()
            
            # Set log level to reduce noise
            self._spark_session.sparkContext.setLogLevel("WARN")
            
            print(f"Spark session started successfully: {self._spark_session.sparkContext.appName}")
            
        except Exception as e:
            print(f"Failed to start Spark session: {str(e)}")
            raise
    
    def get_spark_session(self, app_name: str = "BD_Transformer_Pipeline") -> SparkSession:
        """
        Get current Spark session (start if not running)
        
        Args:
            app_name (str): Name for the Spark application
            
        Returns:
            SparkSession: Active Spark session
        """
        if self._spark_session is None:
            self.start_spark_session(app_name)
        
        # Check if session is still active
        try:
            self._spark_session.sparkContext.statusTracker()
        except Exception:
            print("Spark session appears to be dead, restarting...")
            self._spark_session = None
            self.start_spark_session(app_name)
        
        return self._spark_session
    
    def read_parquet(self, path: str) -> DataFrame:
        """
        Read parquet file using managed Spark session
        
        Args:
            path (str): Path to parquet file/directory
            
        Returns:
            DataFrame: Spark DataFrame
        """
        spark = self.get_spark_session()
        
        try:
            df = spark.read.parquet(path)
            print(f"Successfully read parquet: {path}")
            return df
        except Exception as e:
            print(f"Failed to read parquet {path}: {str(e)}")
            raise
    
    def read_from_postgres(self, table_name: str, ds: str, columns: str = "*") -> DataFrame:
        """
        Read PostgreSQL table into Spark DataFrame
        
        Args:
            table_name (str): PostgreSQL table name
            ds (str): Date string (YYYY-MM-DD) for partition filtering
            columns (str): Columns to select (default: all)
            
        Returns:
            DataFrame: Spark DataFrame
        """
        spark = self.get_spark_session()
        
        # Get database connection details from environment
        host = os.getenv('POSTGRES_HOST', 'postgres')
        port = os.getenv('POSTGRES_PORT', '5432')
        database = os.getenv('POSTGRES_DB', 'airflow')
        user = os.getenv('POSTGRES_USER', 'airflow')
        password = os.getenv('POSTGRES_PASSWORD', 'airflow')
        
        jdbc_url = f"jdbc:postgresql://{host}:{port}/{database}"
        
        # Build query with ds filter (partition key)
        query = f"(SELECT {columns} FROM {table_name} WHERE ds = '{ds}') as filtered_data"
        
        try:
            df = spark.read \
                .format("jdbc") \
                .option("url", jdbc_url) \
                .option("dbtable", query) \
                .option("user", user) \
                .option("password", password) \
                .option("driver", "org.postgresql.Driver") \
                .load()
            
            print(f"Successfully read from PostgreSQL: {table_name} (ds: {ds})")
            return df
            
        except Exception as e:
            print(f"Failed to read from PostgreSQL {table_name}: {str(e)}")
            raise
    
    def write_parquet(self, df: DataFrame, path: str, mode: str = "overwrite"):
        """
        Write Spark DataFrame to parquet file
        
        Args:
            df (DataFrame): Spark DataFrame to write
            path (str): Output path for parquet file
            mode (str): Write mode ('overwrite', 'append', etc.)
        """
        try:
            df.write.mode(mode).parquet(path)
            print(f"Successfully wrote parquet: {path}")
        except Exception as e:
            print(f"Failed to write parquet {path}: {str(e)}")
            raise
    
    def write_to_postgres(self, df: DataFrame, table_name: str, mode: str = "append"):
        """
        Write Spark DataFrame directly to PostgreSQL using JDBC (memory-efficient)
        
        Args:
            df (DataFrame): Spark DataFrame to write
            table_name (str): PostgreSQL table name
            mode (str): Write mode ('append', 'overwrite', etc.)
        """
        # Get database connection details from environment
        host = os.getenv('POSTGRES_HOST', 'postgres')
        port = os.getenv('POSTGRES_PORT', '5432')
        database = os.getenv('POSTGRES_DB', 'airflow')
        user = os.getenv('POSTGRES_USER', 'airflow')
        password = os.getenv('POSTGRES_PASSWORD', 'airflow')
        
        jdbc_url = f"jdbc:postgresql://{host}:{port}/{database}"
        
        try:
            df.write \
                .format("jdbc") \
                .option("url", jdbc_url) \
                .option("dbtable", table_name) \
                .option("user", user) \
                .option("password", password) \
                .option("driver", "org.postgresql.Driver") \
                .option("truncate", "false") \
                .mode(mode) \
                .save()
            
            print(f"Successfully wrote to PostgreSQL: {table_name}")
            
        except Exception as e:
            print(f"Failed to write to PostgreSQL {table_name}: {str(e)}")
            raise
    
    def stop_spark_session(self):
        """
        Stop Spark session and cleanup resources
        """
        if self._spark_session is not None:
            try:
                app_name = self._spark_session.sparkContext.appName
                self._spark_session.stop()
                self._spark_session = None
                print(f"Spark session stopped successfully: {app_name}")
            except Exception as e:
                print(f"Error stopping Spark session: {str(e)}")
        else:
            print("No Spark session to stop")
    
