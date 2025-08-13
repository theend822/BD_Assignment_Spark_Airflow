from dag_utils.spark.SparkManager import SparkManager


def stop_spark_session(app_name:str, spark_config=None):
    """
    Stop Spark session and cleanup resources
    
    This should be called at the end of the DAG to properly cleanup Spark resources.
    """
    spark_manager = SparkManager(app_name, spark_config)
    spark_manager.stop_spark_session()
    
    return {"status": "stopped", "message": "Spark session stopped successfully"}