# dags/bd_transformer_pipeline.py

import sys
import os
sys.path.insert(0, '/opt/airflow')

from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator

from operators.FitTransformOperator import FitTransformOperator
from operators.InverseTransformOperator import InverseTransformOperator

# Import utility functions
from dag_utils.postgres.create_table import create_table
from dag_utils.postgres.ingest_data import ingest_data_from_parquet
from dag_utils.postgres.run_dq_check import run_dq_check
from dag_utils.spark.start_spark_session import start_spark_session
from dag_utils.spark.stop_spark_session import stop_spark_session

# Import DQ check SQL generation function
from dq_checks.check_no_null_values import generate_null_dq_check_sql
from dq_checks.check_row_count_consistency import generate_row_cnt_consistency_dq_check_sql

# Import column name utility
from dag_utils.helper.get_column_names import get_columns_from_config

# Default arguments
default_args = {
    'owner': 'jxy',
    'depends_on_past': False,
    'start_date': datetime(2025, 8, 13),
    'retries': 1,
}

"""
PIPELINE STEPS:
1. Create raw tables in PostgreSQL
2. Read raw data from Parquet files and Load into PostgreSQL
3. Create transformed and inverted tables in PostgreSQL
4. Start Spark session with custom config
5. Read from raw table, fit and transform and Save data into transformed table
6. DQ Check: Check for null values in transformed table
7. Read from transformed table, inverse transform and Save data into inverted table
8. DQ Check: Check for null values in inverted table
9. DQ Check: Check row counts match across all tables
10. Stop Spark session to cleanup resources
"""


# DAG definition
dag = DAG(
    'bd_transformer_pipeline',
    default_args=default_args,
    schedule_interval=None,  # Manual trigger only
    params={
        'app_name': 'BD_Transformer_Pipeline',
        'spark_config': {
            'driver_memory': '4g',
            'executor_memory': '4g',
            'max_result_size': '2g',
        },
        'config_file': '/opt/airflow/config/bd_customer_profiles.yaml',
        'table_names':{
            'raw': 'bd_customer_profiles_raw',
            'transformed': 'bd_customer_profiles_transformed',
            'inverted': 'bd_customer_profiles_inverted',
        }
    }
)

# Task definitions
with dag:

    # Create raw tables in postgres
    create_raw_table = PythonOperator(
        task_id='create_raw_table',
        python_callable=create_table,
        op_kwargs={
            'table_schema': '/opt/airflow/table_schema/{{ params.table_names.raw }}.sql',
            'log_message': 'Raw table created successfully'
        }
    )

    # Load raw data to PostgreSQL
    load_raw_data = PythonOperator(
        task_id='load_raw_data',
        python_callable=ingest_data_from_parquet,
        op_kwargs={
            'parquet_path': "/opt/data/input/",
            'table_name': '{{ params.table_names.raw }}',
            'ds': "{{ ds }}",
            'if_exists': 'overwrite',
            'postgres_config': {
                'host': 'postgres',
                'port': '5432',
                'database': 'bd_datamart',
                'user': 'theend822',
                'password': 'abc123',
            }
        }
    )

    # Create transformed and inverted tables in postgres
    create_transformed_table = PythonOperator(
        task_id='create_transformed_table',
        python_callable=create_table,
        op_kwargs={
            'table_schema': '/opt/airflow/table_schema/{{ params.table_names.transformed }}.sql',
            'log_message': 'Transformed table created successfully'
        }
    )
    
    create_inverted_table = PythonOperator(
        task_id='create_inverted_table',
        python_callable=create_table,
        op_kwargs={
            'table_schema': '/opt/airflow/table_schema/{{ params.table_names.inverted }}.sql',
            'log_message': 'Inverted table created successfully'
        }
    )
    
    # Start Spark session for the entire pipeline with custom config
    start_spark = PythonOperator(
        task_id='start_spark_session',
        python_callable=start_spark_session,
        op_kwargs={
            'app_name': '{{ params.app_name }}',
            'spark_config': '{{ params.spark_config | tojson }}'
        }
    )
    
    # Fit transformer and transform data
    fit_transform = FitTransformOperator(
        task_id='fit_transform_data',
        config_path="{{ params.config_file }}",
        source_table='{{ params.table_names.raw }}',
        target_table='{{ params.table_names.transformed }}',
        postgres_config={
            'host': 'postgres',
            'port': '5432',
            'database': 'bd_datamart',
            'user': 'theend822',
            'password': 'abc123',
        }
    )

    # DQ Check: Check for null values in transformed table
    dq_check_nulls_transformed = PythonOperator(
        task_id='dq_check_nulls_transformed',
        python_callable=run_dq_check,
        op_kwargs={
            'check_name': 'No null values in key columns',
            'sql_query': generate_null_dq_check_sql(col_list=get_columns_from_config("/opt/airflow/config/bd_customer_profiles.yaml"), ds="{{ ds }}", source_table="{{ params.table_names.transformed }}")
        }
    )
    # Inverse transform data
    inverse_transform = InverseTransformOperator(
        task_id='inverse_transform_data',
        config_path="{{ params.config_file }}",
        source_table='{{ params.table_names.transformed }}',
        target_table='{{ params.table_names.inverted }}',
        raw_table='{{ params.table_names.raw }}', # required by inverse transform operator
        postgres_config={
            'host': 'postgres',
            'port': '5432',
            'database': 'bd_datamart',
            'user': 'theend822',
            'password': 'abc123',
        }
    )

    # DQ Check: Check for null values in inverted table
    dq_checks_nulls_inverted = PythonOperator(
        task_id='dq_checks_nulls_inverted',
        python_callable=run_dq_check,
        op_kwargs={
            'check_name': 'No null values in key columns',
            'sql_query': generate_null_dq_check_sql(col_list=get_columns_from_config("/opt/airflow/config/bd_customer_profiles.yaml", suffix_list=["_data", "_valid", "_error"]), ds="{{ ds }}", source_table="{{ params.table_names.inverted }}")
        }
    )
    
    # DQ Check: Check row counts match across all tables
    dq_check_row_counts = PythonOperator(
        task_id='dq_check_row_counts',
        python_callable=run_dq_check,
        op_kwargs={
            'check_name': 'Row counts match across all tables',
            'sql_query': generate_row_cnt_consistency_dq_check_sql(table_list="{{ params.table_names.values() | list }}", ds="{{ ds }}")
        }
    )
    
    
    # Stop Spark session to cleanup resources (final step)
    stop_spark = PythonOperator(
        task_id='stop_spark_session',
        python_callable=stop_spark_session,
        op_kwargs={
            'app_name': '{{ params.app_name }}',
            'spark_config': '{{ params.spark_config | tojson }}'
        }
    )
    
    # Task dependencies
    create_raw_table >> load_raw_data >> [create_transformed_table, create_inverted_table] >> start_spark >> fit_transform >> dq_check_nulls_transformed >> inverse_transform >> dq_checks_nulls_inverted >> dq_check_row_counts >> stop_spark
