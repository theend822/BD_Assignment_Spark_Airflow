# dags/bd_transformer_pipeline.py

from datetime import datetime, timedelta
from airflow import DAG
from airflow.utils.task_group import TaskGroup

from dag_utils.bd_transformer_operators.fit_operator import ColumnFitOperator
from dag_utils.bd_transformer_operators.transform_operator import TransformOperator
from dag_utils.bd_transformer_operators.data_operator import DataLoadOperator
from dag_utils.bd_transformer_operators.collect_operator import CollectFitResultsOperator

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
    
    # Load data
    load_data = DataLoadOperator(
        task_id='load_data',
        input_path="{{ params.input_path }}",
    )
    
    # Parallel column fitting using TaskGroup
    with TaskGroup('fit_columns_parallel') as fit_group:
        
        columns = [
            'day_of_month', 'height', 'account_balance', 
            'net_profit', 'customer_ratings', 'leaderboard_rank'
        ]
        
        fit_tasks = []
        for column in columns:
            fit_task = ColumnFitOperator(
                task_id=f'fit_{column}',
                column_name=column,
                config_path="{{ params.config_path }}",
            )
            fit_tasks.append(fit_task)
    
    # Collect fit results
    collect_fits = CollectFitResultsOperator(
        task_id='collect_fit_results',
        columns=columns,
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
    
    # Define task dependencies
    load_data >> fit_group >> collect_fits >> transform_data >> inverse_transform_data