from typing import Dict, Any
from airflow.models import BaseOperator
from airflow.utils.context import Context
from pyspark.sql import SparkSession


class DataLoadOperator(BaseOperator):
    """Operator to load data for processing"""
    
    def __init__(
        self,
        input_path: str,
        **kwargs
    ):
        super().__init__(**kwargs)
        self.input_path = input_path
        
    def get_spark_session(self) -> SparkSession:
        """Create Spark session"""
        spark = SparkSession.builder \
            .appName("BD_Transformer_DataLoad") \
            .config("spark.driver.memory", "2g") \
            .config("spark.executor.memory", "2g") \
            .getOrCreate()
        spark.sparkContext.setLogLevel("WARN")
        return spark
        
    def execute(self, context: Context) -> Dict[str, Any]:
        """Load data and prepare for processing"""
        self.log.info(f"Loading data from: {self.input_path}")
        
        spark = None
        try:
            # Setup Spark
            spark = self.get_spark_session()
            
            # Load data
            df = spark.read.parquet(self.input_path)
            
            # Get basic info
            row_count = df.count()
            column_count = len(df.columns)
            
            self.log.info(f"Loaded data: {row_count} rows, {column_count} columns")
            
            # Data info to pass to subsequent tasks
            data_info = {
                'path': self.input_path,
                'row_count': row_count,
                'column_count': column_count,
            }
            
            # Save data info to XCom
            context['task_instance'].xcom_push(key='data_info', value=data_info)
            
            self.log.info("Data loading completed successfully")
            
            return data_info
            
        except Exception as e:
            self.log.error(f"Error loading data: {str(e)}")
            raise
        finally:
            if spark:
                spark.stop()