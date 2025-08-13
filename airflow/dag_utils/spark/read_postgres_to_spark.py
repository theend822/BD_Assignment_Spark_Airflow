from dag_utils.SparkManager import SparkManager


def read_postgres_to_spark(table_name, run_id, columns="*"):
    """
    Read PostgreSQL table into Spark DataFrame
    
    Args:
        table_name (str): PostgreSQL table name
        run_id (str): Pipeline run ID to filter data
        columns (str): Columns to select (default: all)
    
    Returns:
        dict: Information about the read operation
    """
    spark_manager = SparkManager()
    
    try:
        # Read data from PostgreSQL
        df = spark_manager.read_from_postgres(table_name, run_id, columns)
        
        # Get basic info about the DataFrame
        row_count = df.count()
        column_count = len(df.columns)
        
        return {
            "status": "success",
            "table_name": table_name,
            "run_id": run_id,
            "row_count": row_count,
            "column_count": column_count,
            "columns": df.columns
        }
        
    except Exception as e:
        return {
            "status": "error",
            "table_name": table_name,
            "run_id": run_id,
            "error": str(e)
        }