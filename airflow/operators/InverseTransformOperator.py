from typing import Dict, Any
from airflow.models import BaseOperator
from airflow.utils.context import Context
from dag_utils.spark.SparkManager import SparkManager
from bd_transformer.transformer import Transformer
import yaml


class InverseTransformOperator(BaseOperator):
    """
        - Reads transformed data from PostgreSQL
        - Recreates fitted transformer (by fitting on raw data)
        - Applies inverse transformation
        - Saves inverted data to PostgreSQL
    """
    
    def __init__(
        self,
        config_path: str,
        source_table: str,
        target_table: str,
        raw_table: str,
        **kwargs
    ):
        super().__init__(**kwargs)
        self.config_path = config_path
        self.source_table = source_table
        self.target_table = target_table
        self.raw_table = raw_table

    
    def load_config(self, config_path: str) -> Dict[str, Any]:
        """Load configuration from local path"""
        with open(config_path, 'r') as f:
            return yaml.safe_load(f)
        
    def execute(self, context: Context):
        """Load fitted transformer and inverse transform data"""
        self.log.info("Starting inverse transform operation")
        
        try:
            # Get Spark config from DAG params
            app_name = context['params']['app_name']
            spark_config = context['params']['spark_config']
            
            # Use shared Spark session
            spark_manager = SparkManager(app_name, spark_config)
            
            # Load configuration to recreate transformer
            config = self.load_config(self.config_path)
            self.log.info(f"Loaded config for inverse transform: {list(config.keys())}")
            
            # ds is automatically provided by Airflow context
            ds = context['ds']
            
            # Read transformed data from PostgreSQL
            transformed_df = spark_manager.read_from_postgres(self.source_table, ds)
            
            row_count = transformed_df.count()
            self.log.info(f"Loaded {row_count} transformed rows from {self.source_table}")
            
            # We need the fitted transformer - recreate it by reading raw data and fitting again
            # This is necessary because we removed pickle approach
            raw_df = spark_manager.read_from_postgres(self.raw_table, ds)
            transformer = Transformer(config)
            fitted_transformer = transformer.fit(raw_df)
            
            # Apply inverse transform
            inverted_df = fitted_transformer.inverse_transform(transformed_df)
            
            # Save inverted data to PostgreSQL
            spark_manager.write_to_postgres(inverted_df, self.target_table, mode='replace')
            
            self.log.info(f"Successfully inverse transformed {row_count} rows and saved to {self.target_table}")
            
        except Exception as e:
            self.log.error(f"Error in inverse transform: {str(e)}")
            raise