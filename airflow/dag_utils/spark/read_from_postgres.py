from dag_utils.spark.SparkManager import SparkManager
from pyspark.sql import DataFrame
from typing import List, Union, Optional


def read_from_postgres(table_name: str, ds: str, app_name: str, columns: Union[List[str], str] = "*", spark_config: Optional[dict] = None) -> DataFrame:
    """
    Read PostgreSQL table into Spark DataFrame
    
    Args:
        table_name (str): PostgreSQL table name
        ds (str): Date string (YYYY-MM-DD) for partition filtering
        app_name (str): Spark application name
        columns (List[str] or str("*")): Columns to select - either list of column names or default value "*" for all
        spark_config (dict): Spark configuration overrides
    
    Returns:ß
        DataFrame: Spark DataFrame with the table data
    """
    spark_manager = SparkManager(app_name, spark_config)
    return spark_manager.read_from_postgres(table_name, ds, columns)