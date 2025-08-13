# dags/bd_transformer_pipeline.py

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

from operators.DataOperator import DataLoadOperator
from operators.TransformerFitOperator import TransformerFitOperator
from operators.TransformOperator import TransformOperator

# Import utility functions
from dag_utils.postgres.create_table import create_table
from dag_utils.postgres.ingest_data import ingest_data_from_parquet
from dag_utils.postgres.run_dq_check import run_pipeline_dq_checks
from dag_utils.spark.start_spark_session import start_spark_session
from dag_utils.spark.stop_spark_session import stop_spark_session

# Default arguments
default_args = {
    'owner': 'jxy',
    'depends_on_past': False,
    'start_date': datetime(2025, 8, 13),
    'retries': 1,
}

# DAG definition
dag = DAG(
    'bd_transformer_pipeline',
    default_args=default_args,
    schedule_interval=None,  # Manual trigger only
    tags=['data-processing', 'spark'],
    params={
        'input_path': '/opt/data/input/',
        'output_path': '/opt/data/output/',
        'config_path': '/opt/airflow/config/bd_transformer_config.yaml',
    }
)

# Task definitions
with dag:
    
    # Start Spark session for the entire pipeline
    start_spark = PythonOperator(
        task_id='start_spark_session',
        python_callable=start_spark_session
    )
    
    # Create tables
    create_raw_table = PythonOperator(
        task_id='create_raw_table',
        python_callable=create_table,
        op_kwargs={
            'table_schema': '/opt/airflow/table_schema/bd_customer_profiles_raw.sql',
            'log_message': 'BD Customer Profiles Raw table created successfully'
        }
    )
    
    create_transformed_table = PythonOperator(
        task_id='create_transformed_table',
        python_callable=create_table,
        op_kwargs={
            'table_schema': '/opt/airflow/table_schema/bd_customer_profiles_transformed.sql',
            'log_message': 'BD Customer Profiles Transformed table created successfully'
        }
    )
    
    create_inverted_table = PythonOperator(
        task_id='create_inverted_table',
        python_callable=create_table,
        op_kwargs={
            'table_schema': '/opt/airflow/table_schema/bd_customer_profiles_inverted.sql',
            'log_message': 'BD Customer Profiles Inverted table created successfully'
        }
    )
    
    # Load data
    load_data = DataLoadOperator(
        task_id='load_data',
        input_path="{{ params.input_path }}",
    )
    
    # Fit transformer for all columns at once
    fit_transformer = TransformerFitOperator(
        task_id='fit_transformer',
        config_path="{{ params.config_path }}",
    )
    
    # Transform data
    transform_data = TransformOperator(
        task_id='transform_data',
        operation='transform',
        output_path="{{ params.output_path }}/transformed/",
    )
    
    # Inverse transform
    inverse_transform_data = TransformOperator(
        task_id='inverse_transform_data',
        operation='inverse_transform',
        output_path="{{ params.output_path }}/recovered/",
    )
    
    # Save raw data to PostgreSQL
    save_raw = PythonOperator(
        task_id='save_raw_data',
        python_callable=ingest_data_from_parquet,
        op_kwargs={
            'parquet_path': "{{ params.input_path }}",
            'table_name': 'bd_customer_profiles_raw',
            'run_id': "{{ run_id }}",
            'if_exists': 'append'
        }
    )
    
    # Save transformed data to PostgreSQL
    save_transformed = PythonOperator(
        task_id='save_transformed_data',
        python_callable=ingest_data_from_parquet,
        op_kwargs={
            'parquet_path': "{{ params.output_path }}/transformed/",
            'table_name': 'bd_customer_profiles_transformed',
            'run_id': "{{ run_id }}",
            'if_exists': 'append'
        }
    )
    
    # Save inverted data to PostgreSQL
    save_inverted = PythonOperator(
        task_id='save_inverted_data',
        python_callable=ingest_data_from_parquet,
        op_kwargs={
            'parquet_path': "{{ params.output_path }}/recovered/",
            'table_name': 'bd_customer_profiles_inverted',
            'run_id': "{{ run_id }}",
            'if_exists': 'append'
        }
    )
    
    # Run data quality checks
    run_dq_checks = PythonOperator(
        task_id='run_dq_checks',
        python_callable=run_pipeline_dq_checks,
        op_kwargs={
            'run_id': "{{ run_id }}"
        }
    )
    
    # Stop Spark session to cleanup resources
    stop_spark = PythonOperator(
        task_id='stop_spark_session',
        python_callable=stop_spark_session
    )
    
    # Define task dependencies for full end-to-end pipeline
    
    # Start with Spark session and table creation
    start_spark >> [create_raw_table, create_transformed_table, create_inverted_table] >> load_data
    
    # Main processing pipeline
    load_data >> fit_transformer >> transform_data >> inverse_transform_data
    
    # Database saves (parallel)
    load_data >> save_raw
    transform_data >> save_transformed  
    inverse_transform_data >> save_inverted
    
    # Final DQ check after all data is saved, then stop Spark
    [save_raw, save_transformed, save_inverted] >> run_dq_checks >> stop_spark