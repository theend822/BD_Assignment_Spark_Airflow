from dag_utils.SparkManager import SparkManager


def start_spark_session():
    """
    Start Spark session for the pipeline
    
    This should be called at the beginning of the DAG to initialize
    the shared Spark session that will be used by all tasks.
    """
    spark_manager = SparkManager()
    spark_manager.start_spark_session("BD_Transformer_Pipeline")
    
    # Return session info for logging
    return spark_manager.get_session_info()