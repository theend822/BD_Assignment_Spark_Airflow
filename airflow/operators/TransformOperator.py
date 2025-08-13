from typing import Dict, Any
from airflow.models import BaseOperator
from airflow.utils.context import Context
from dag_utils.spark.SparkManager import SparkManager
from bd_transformer.transformer import Transformer


class TransformOperator(BaseOperator):
    """Operator to perform transform or inverse_transform operations using shared Spark session"""
    
    def __init__(
        self,
        operation: str,  # 'transform' or 'inverse_transform'
        output_path: str,
        **kwargs
    ):
        super().__init__(**kwargs)
        self.operation = operation
        self.output_path = output_path
        
        if operation not in ['transform', 'inverse_transform']:
            raise ValueError("Operation must be 'transform' or 'inverse_transform'")
    
    def load_fitted_transformer(self) -> Transformer:
        """Load the pre-fitted transformer from pickle file"""
        import pickle
        
        transformer_path = "/opt/airflow/fitted_transformer.pkl"
        with open(transformer_path, 'rb') as f:
            transformer = pickle.load(f)
        
        return transformer
    
    def execute(self, context: Context) -> Dict[str, Any]:
        """Execute transform or inverse_transform operation"""
        self.log.info(f"Starting {self.operation} operation")
        
        try:
            # Use shared Spark session
            spark_manager = SparkManager()
            
            # Load the fitted transformer
            transformer = self.load_fitted_transformer()
            self.log.info("Loaded fitted transformer from pickle file")
            
            # Get ds from context (partition key)
            ds = context['ds']
            
            if self.operation == 'transform':
                # Read raw data from PostgreSQL
                df = spark_manager.read_from_postgres('bd_customer_profiles_raw', ds)
                
                # Remove run_id column as it's not needed for transformer
                if 'run_id' in df.columns:
                    df = df.drop('run_id')
                
                # Perform transformation
                result_df = transformer.transform(df)
                
            else:  # inverse_transform
                # Load transformed data from parquet
                transform_info = context['task_instance'].xcom_pull(key='transform_info', task_ids='transform_data')
                result_df = spark_manager.read_parquet(transform_info['path'])
                
                # Perform inverse transformation
                result_df = transformer.inverse_transform(result_df)
            
            # Save results to parquet
            spark_manager.write_parquet(result_df, self.output_path)
            
            # Get result stats
            row_count = result_df.count()
            column_count = len(result_df.columns)
            
            result_info = {
                'path': self.output_path,
                'row_count': row_count,
                'column_count': column_count,
                'operation': self.operation
            }
            
            # Save result info to XCom
            context['task_instance'].xcom_push(key=f'{self.operation}_info', value=result_info)
            
            self.log.info(f"Successfully completed {self.operation}")
            self.log.info(f"Output: {row_count} rows, {column_count} columns")
            self.log.info(f"Saved to: {self.output_path}")
            
            return result_info
            
        except Exception as e:
            self.log.error(f"Error during {self.operation}: {str(e)}")
            raise