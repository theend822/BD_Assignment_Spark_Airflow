from dag_utils.spark.SparkManager import SparkManager


def start_spark_session(app_name:str, spark_config=None):
    """
    Start Spark session for the pipeline
    
    Args:
        app_name (str): Name for the Spark application
        spark_config (dict, optional): Spark configuration overrides
    
    This should be called at the beginning of the DAG to initialize
    the shared Spark session that will be used by all tasks.
    """
    spark_manager = SparkManager(app_name, spark_config)
    spark_manager.start_spark_session()
    
    return {"status": "started", "message": "Spark session started successfully"}