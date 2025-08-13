from typing import Dict, Any
from airflow.models import BaseOperator
from airflow.utils.context import Context
from dag_utils.spark.SparkManager import SparkManager
from bd_transformer.transformer import Transformer
import yaml
import pickle
import os


class TransformerFitOperator(BaseOperator):
    """Operator to fit BD Transformer reading from PostgreSQL"""
    
    def __init__(
        self,
        config_path: str,
        **kwargs
    ):
        super().__init__(**kwargs)
        self.config_path = config_path
    
    def load_config(self, config_path: str) -> Dict[str, Any]:
        """Load configuration from local path"""
        with open(config_path, 'r') as f:
            return yaml.safe_load(f)
        
    def execute(self, context: Context) -> Dict[str, Any]:
        """Fit transformer for all configured columns"""
        self.log.info("Starting fit operation for all columns")
        
        try:
            # Use shared Spark session
            spark_manager = SparkManager()
            
            # Load configuration
            config = self.load_config(self.config_path)
            self.log.info(f"Loaded config for columns: {list(config.keys())}")
            
            # Get ds from context (partition key)
            ds = context['ds']
            
            # Read data from PostgreSQL instead of parquet (eliminates double read!)
            df = spark_manager.read_from_postgres('bd_customer_profiles_raw', ds)
            
            # Remove run_id column as it's not needed for transformer
            if 'run_id' in df.columns:
                df = df.drop('run_id')
            
            row_count = df.count()
            self.log.info(f"Loaded data from PostgreSQL with {row_count} rows for fitting all columns")
            
            # Initialize and fit transformer
            transformer = Transformer(config)
            fitted_transformer = transformer.fit(df)
            
            # Serialize the fitted transformer for later use
            transformer_path = "/opt/airflow/fitted_transformer.pkl"
            with open(transformer_path, 'wb') as f:
                pickle.dump(fitted_transformer, f)
            
            # Prepare summary for logging and downstream tasks
            fit_summary = {
                'transformer_path': transformer_path,
                'fitted_columns': list(config.keys()),
                'total_columns': len(config),
                'data_rows': row_count,
                'data_source': 'postgresql'
            }
            
            # Save summary to XCom
            context['task_instance'].xcom_push(key='transformer_fit_info', value=fit_summary)
            
            self.log.info(f"Successfully fitted transformer for {len(config)} columns")
            self.log.info(f"Fitted columns: {list(config.keys())}")
            self.log.info(f"Transformer saved to: {transformer_path}")
            
            return fit_summary
            
        except Exception as e:
            self.log.error(f"Error fitting transformer: {str(e)}")
            raise