from typing import Dict, Any
from airflow.models import BaseOperator
from airflow.utils.context import Context
from pyspark.sql import SparkSession
from bd_transformer.transformer import Transformer
from bd_transformer.components.converter import Converter
from bd_transformer.components.normalizer import Normalizer


class TransformOperator(BaseOperator):
    """Operator to perform transform or inverse_transform operations"""
    
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
    
    def get_spark_session(self) -> SparkSession:
        """Create Spark session"""
        spark = SparkSession.builder \
            .appName(f"BD_Transformer_{self.operation}") \
            .config("spark.driver.memory", "2g") \
            .config("spark.executor.memory", "2g") \
            .getOrCreate()
        spark.sparkContext.setLogLevel("WARN")
        return spark
    
    def _reconstruct_transformer(self, all_fit_results: Dict[str, Any]) -> Transformer:
        """Reconstruct transformer from fit results"""
        # Create config from fit results
        config = {}
        for column_name, fit_result in all_fit_results.items():
            config[column_name] = fit_result['config']
        
        # Create transformer
        transformer = Transformer(config)
        
        # Manually set the fitted parameters
        for column_name, fit_result in all_fit_results.items():
            # Reconstruct converter
            converter_params = fit_result['converter_params']
            converter = Converter(
                min_val=converter_params['min_val'],
                max_val=converter_params['max_val'],
                clip_oor=converter_params['clip_oor'],
                prefix=converter_params['prefix'],
                suffix=converter_params['suffix'],
                rounding=converter_params['rounding']
            )
            # Set fitted parameters
            converter._min_val = converter_params['min_val']
            converter._max_val = converter_params['max_val']
            converter._original_type = converter_params['original_type']
            
            # Reconstruct normalizer
            normalizer_params = fit_result['normalizer_params']
            normalizer = Normalizer(
                clip=normalizer_params['clip'],
                reject=normalizer_params['reject']
            )
            # Set fitted parameters
            normalizer._min = normalizer_params['min']
            normalizer._max = normalizer_params['max']
            normalizer._scale = normalizer_params['scale']
            
            # Add to transformer
            transformer.converters[column_name] = converter
            transformer.normalizers[column_name] = normalizer
        
        return transformer
    
    def execute(self, context: Context) -> Dict[str, Any]:
        """Execute transform or inverse_transform operation"""
        self.log.info(f"Starting {self.operation} operation")
        
        spark = None
        try:
            # Setup Spark
            spark = self.get_spark_session()
            
            # Load fit results
            all_fit_results = context['task_instance'].xcom_pull(key='all_fit_results', task_ids='collect_fit_results')
            if not all_fit_results:
                raise ValueError("No fit results available")
            
            # Reconstruct transformer from fit results
            transformer = self._reconstruct_transformer(all_fit_results)
            
            if self.operation == 'transform':
                # Load original data
                data_info = context['task_instance'].xcom_pull(key='data_info', task_ids='load_data')
                df = spark.read.parquet(data_info['path'])
                
                # Perform transformation
                result_df = transformer.transform(df)
                
            else:  # inverse_transform
                # Load transformed data
                transform_info = context['task_instance'].xcom_pull(key='transform_info', task_ids='transform_data')
                df = spark.read.parquet(transform_info['path'])
                
                # Perform inverse transformation
                result_df = transformer.inverse_transform(df)
            
            # Save results
            result_df.write.mode('overwrite').parquet(self.output_path)
            
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
        finally:
            if spark:
                spark.stop()