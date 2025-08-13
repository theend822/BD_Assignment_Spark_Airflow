from typing import Dict, Any
from airflow.models import BaseOperator
from airflow.utils.context import Context
from dag_utils.spark.SparkManager import SparkManager


class DataLoadOperator(BaseOperator):
    """Operator to load data for processing using shared Spark session"""
    
    def __init__(
        self,
        input_path: str,
        **kwargs
    ):
        super().__init__(**kwargs)
        self.input_path = input_path
        
    def execute(self, context: Context) -> Dict[str, Any]:
        """Load data and prepare for processing"""
        self.log.info(f"Loading data from: {self.input_path}")
        
        try:
            # Use shared Spark session
            spark_manager = SparkManager()
            
            # Load data
            df = spark_manager.read_parquet(self.input_path)
            
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