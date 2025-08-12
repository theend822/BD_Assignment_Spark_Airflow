from typing import Dict, Any
from airflow.models import BaseOperator
from airflow.utils.context import Context
from pyspark.sql import SparkSession
from bd_transformer.components.converter import Converter
from bd_transformer.components.normalizer import Normalizer
import yaml
import pickle


class ColumnFitOperator(BaseOperator):
    """Operator to fit BD Transformer components for a single column"""
    
    def __init__(
        self,
        column_name: str,
        config_path: str,
        **kwargs
    ):
        super().__init__(**kwargs)
        self.column_name = column_name
        self.config_path = config_path
        
    def get_spark_session(self) -> SparkSession:
        """Create Spark session"""
        spark = SparkSession.builder \
            .appName(f"BD_Transformer_Fit_{self.column_name}") \
            .config("spark.driver.memory", "2g") \
            .config("spark.executor.memory", "2g") \
            .getOrCreate()
        spark.sparkContext.setLogLevel("WARN")
        return spark
    
    def load_config(self, config_path: str) -> Dict[str, Any]:
        """Load configuration from local path"""
        with open(config_path, 'r') as f:
            return yaml.safe_load(f)
        
    def execute(self, context: Context) -> Dict[str, Any]:
        """Fit converter and normalizer for the specified column"""
        self.log.info(f"Starting fit operation for column: {self.column_name}")
        
        spark = None
        try:
            # Setup Spark
            spark = self.get_spark_session()
            
            # Load configuration
            full_config = self.load_config(self.config_path)
            column_config = full_config.get(self.column_name, {})
            
            if not column_config:
                raise ValueError(f"No configuration found for column: {self.column_name}")
            
            # Load data from previous task
            data_info = context['task_instance'].xcom_pull(key='data_info', task_ids='load_data')
            df = spark.read.parquet(data_info['path'])
            
            self.log.info(f"Loaded data with {df.count()} rows for fitting {self.column_name}")
            
            # Initialize and fit converter
            converter_config = column_config.get('converter', {})
            converter = Converter(**converter_config)
            converter.fit(df, self.column_name)
            
            # Convert data for normalizer fitting
            converted_df = converter.convert(df, self.column_name)
            
            # Initialize and fit normalizer
            normalizer_config = column_config.get('normalizer', {})
            normalizer = Normalizer(**normalizer_config)
            normalizer.fit(converted_df, self.column_name)
            
            # Prepare fit results
            fit_results = {
                'column_name': self.column_name,
                'converter_params': {
                    'min_val': converter._min_val,
                    'max_val': converter._max_val,
                    'clip_oor': converter._clip_oor,
                    'prefix': converter._prefix,
                    'suffix': converter._suffix,
                    'rounding': converter._rounding,
                    'original_type': converter._original_type
                },
                'normalizer_params': {
                    'min': normalizer._min,
                    'max': normalizer._max,
                    'scale': normalizer._scale,
                    'clip': normalizer._clip,
                    'reject': normalizer._reject
                },
                'config': column_config
            }
            
            # Save to XCom
            context['task_instance'].xcom_push(key=f'fit_results_{self.column_name}', value=fit_results)
            
            self.log.info(f"Successfully fitted column: {self.column_name}")
            self.log.info(f"Converter min/max: {converter._min_val}/{converter._max_val}")
            self.log.info(f"Normalizer min/max/scale: {normalizer._min}/{normalizer._max}/{normalizer._scale}")
            
            return fit_results
            
        except Exception as e:
            self.log.error(f"Error fitting column {self.column_name}: {str(e)}")
            raise
        finally:
            if spark:
                spark.stop()