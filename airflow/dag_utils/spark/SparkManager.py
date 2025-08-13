import os
from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from typing import Optional, Union, List


class SparkManager:
    """
    Centralized Spark session management for Airflow pipeline
    
    Key Features:
    - One Spark session per DAG worker (allows concurrent DAGs)
    - Centralized configuration and connection management
    - PostgreSQL JDBC integration for reading database tables
    - Consistent error handling across all Spark operations
    """
    
    def __init__(self, app_name: str, spark_config: Optional[dict], postgres_config: Optional[dict] = None):
        """Initialize SparkManager for each DAG worker"""
        self._spark_session = None
        self._app_name = app_name
        self._spark_config = spark_config or {}
        self._postgres_config = postgres_config or {}
    
    def start_spark_session(self):
        """
        Start Spark session with configurable settings
        """
        if self._spark_session is not None:
            print(f"Spark session already running: {self._spark_session.sparkContext.appName}")
            return
        
        try:
            print(f"Starting Spark session: {self._app_name}")
            
            self._spark_session = SparkSession.builder \
                .appName(self._app_name) \
                .config("spark.driver.memory", self._spark_config.get("driver_memory", "2g")) \
                .config("spark.executor.memory", self._spark_config.get("executor_memory", "2g")) \
                .config("spark.driver.maxResultSize", self._spark_config.get("max_result_size", "1g")) \
                .config("spark.sql.adaptive.enabled", self._spark_config.get("adaptive_query", "true")) \
                .config("spark.sql.adaptive.coalescePartitions.enabled", self._spark_config.get("coalesce_partitions", "true")) \
                .config("spark.jars", self._spark_config.get("jars", "/opt/spark/jars/postgresql-42.6.0.jar")) \
                .getOrCreate()
            
            # Set log level to reduce noise
            self._spark_session.sparkContext.setLogLevel("WARN")
            
            print(f"Spark session started successfully: {self._spark_session.sparkContext.appName}")
            
        except Exception as e:
            print(f"Failed to start Spark session: {str(e)}")
            raise
    
    def get_spark_session(self) -> SparkSession:
        """
        Get current Spark session (reuse existing shared session if available)
        
        Returns:
            SparkSession: Active Spark session
        """
        # First try to get existing shared session
        try:
            shared_session_id = os.getenv('SHARED_SPARK_SESSION_ID')
            if shared_session_id:
                active_session = SparkSession.getActiveSession()
                if active_session and active_session.sparkContext.applicationId == shared_session_id:
                    print(f"Reusing shared Spark session: {shared_session_id}")
                    return active_session
        except Exception as e:
            print(f"Could not reuse shared session: {e}")
        
        # Fallback to creating new session
        if self._spark_session is None:
            self.start_spark_session()
        
        if self._spark_session is None:
            raise RuntimeError("Failed to create Spark session")
            
        return self._spark_session
    
    def read_from_parquet(self, path: str) -> DataFrame:
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
    
    def read_from_postgres(self, table_name: str, ds: str, columns: Union[List[str], str] = "*") -> DataFrame:
        """
        Read PostgreSQL table into Spark DataFrame
        
        Args:
            table_name (str): PostgreSQL table name
            ds (str): Date string (YYYY-MM-DD) for partition filtering
            columns (List[str] or str("*")): Columns to select - either list of column names or default value "*" for all
            
        Returns:
            DataFrame: Spark DataFrame
        """
        spark = self.get_spark_session()
        
        # Convert list of columns to comma-separated string if needed
        if isinstance(columns, list):
            columns_str = ", ".join(columns)
        else:
            columns_str = columns
        
        # Get database connection details from passed config
        host = self._postgres_config.get('host')
        port = self._postgres_config.get('port')
        database = self._postgres_config.get('database')
        user = self._postgres_config.get('user')
        password = self._postgres_config.get('password')
        
        jdbc_url = f"jdbc:postgresql://{host}:{port}/{database}"
        
        # Build query with ds filter (partition key)
        query = f"(SELECT {columns_str} FROM {table_name} WHERE ds = '{ds}') as filtered_data"
        
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
    
    def write_to_parquet(self, df: DataFrame, path: str, mode: str = "overwrite"):
        """
        Write Spark DataFrame to parquet file
        
        Args:
            df (DataFrame): Spark DataFrame to write
            path (str): Output path for parquet file
            mode (str): Write mode ('overwrite', 'append', etc.)
        """
        spark = self.get_spark_session()
        
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
        spark = self.get_spark_session()
        
        # Get database connection details from passed config
        host = self._postgres_config.get('host')
        port = self._postgres_config.get('port')
        database = self._postgres_config.get('database')
        user = self._postgres_config.get('user')
        password = self._postgres_config.get('password')
        
        # Debug: Print all connection details
        print(f"JDBC connection details - host: {host}, port: {port}, database: {database}, user: {user}, password: {'***' if password else None}")
        
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
    
