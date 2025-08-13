from dag_utils.SparkManager import SparkManager


def stop_spark_session():
    """
    Stop Spark session and cleanup resources
    
    This should be called at the end of the DAG to properly
    cleanup Spark resources.
    """
    spark_manager = SparkManager()
    spark_manager.stop_spark_session()
    
    return {"status": "stopped", "message": "Spark session stopped successfully"}