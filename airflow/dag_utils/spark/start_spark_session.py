from dag_utils.spark.SparkManager import SparkManager


def start_spark_session(app_name:str, spark_config=None):
    """
    Start Spark session for the pipeline and store globally for sharing
    
    Args:
        app_name (str): Name for the Spark application
        spark_config (dict or str, optional): Spark configuration overrides
    
    This should be called at the beginning of the DAG to initialize
    the shared Spark session that will be used by all tasks.
    """
    import json
    import os
    
    # Parse JSON string if spark_config is a string
    if isinstance(spark_config, str):
        spark_config = json.loads(spark_config)
    
    spark_manager = SparkManager(app_name, spark_config)
    spark_manager.start_spark_session()
    
    # Store session info for other tasks to use
    spark_session = spark_manager.get_spark_session()
    session_id = spark_session.sparkContext.applicationId
    
    # Store session ID in environment for other tasks
    os.environ['SHARED_SPARK_SESSION_ID'] = session_id
    os.environ['SHARED_SPARK_APP_NAME'] = app_name
    
    return {"status": "started", "message": f"Spark session started successfully: {session_id}"}