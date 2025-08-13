from typing import Dict, Any
from airflow.models import BaseOperator
from airflow.utils.context import Context
from dag_utils.spark.SparkManager import SparkManager
from bd_transformer.transformer import Transformer
import yaml


class FitTransformOperator(BaseOperator):
    """
        - Reads raw data from PostgreSQL
        - Fits the transformer
        - Applies transformation
        - Saves transformed data to PostgreSQL
    """
    
    # customized operator doesn't have this by default. Airflow will not resolve {{ params.xxx }} 
    # but will pass the literal string values
    template_fields = ('config_path', 'source_table', 'target_table')
    
    def __init__(
        self,
        config_path: str,
        source_table: str,
        target_table: str,
        postgres_config: dict = None,
        **kwargs
    ):
        super().__init__(**kwargs)
        self.config_path = config_path
        self.source_table = source_table
        self.target_table = target_table
        self.postgres_config = postgres_config
    
    def load_config(self, config_path: str) -> Dict[str, Any]:
        """Load configuration from local path"""
        with open(config_path, 'r') as f:
            return yaml.safe_load(f)
        
    def execute(self, context: Context):
        """Fit transformer and transform data"""
        self.log.info("Starting fit operation for all columns")
        
        try:
            # Get Spark config from DAG params
            app_name = context['params']['app_name']
            spark_config = context['params']['spark_config']
            
            # Use shared Spark session with postgres config
            postgres_config = self.postgres_config or {}
            self.log.info(f"DEBUG - FitTransformOperator postgres_config: {postgres_config}")
            spark_manager = SparkManager(app_name, spark_config, postgres_config)
            
            # Load configuration
            config = self.load_config(self.config_path)
            self.log.info(f"Loaded config for columns: {list(config.keys())}")
            
            # Get ds from context (partition key)
            ds = context['ds']
            
            # Read data from PostgreSQL (table name parameterized) and cache to reduce I/O
            df = spark_manager.read_from_postgres(self.source_table, ds)
            df.cache()  # Cache the DataFrame to reduce repeated I/O operations
            
            # Initialize and fit transformer
            transformer = Transformer(config)
            fitted_transformer = transformer.fit(df)
            
            # Apply transform to get transformed data
            transformed_df = fitted_transformer.transform(df)
            
            # Save transformed data to PostgreSQL
            spark_manager.write_to_postgres(transformed_df, self.target_table, mode='replace')
            
            row_count = transformed_df.count()
            self.log.info(f"Successfully transformed {row_count} rows and saved to {self.target_table}")
            
        except Exception as e:
            self.log.error(f"Error fitting transformer: {str(e)}")
            raise