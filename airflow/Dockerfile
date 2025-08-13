# docker/Dockerfile

FROM apache/airflow:2.10.5-python3.9

USER root

# Install Java for Spark
RUN apt-get update && \
    apt-get install -y openjdk-11-jdk && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Set Java environment
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64

USER airflow

# Copy requirements and install Python packages
COPY requirements.txt /opt/airflow/
RUN pip install --no-cache-dir -r /opt/airflow/requirements.txt

# Install bd_transformer package from GitHub
RUN pip install --no-cache-dir git+https://github.com/theend822/BD_Assignment_Spark_Package.git

# Download PostgreSQL JDBC driver for Spark
RUN mkdir -p /opt/spark/jars && \
    wget -O /opt/spark/jars/postgresql-42.6.0.jar \
    https://jdbc.postgresql.org/download/postgresql-42.6.0.jar

# Set environment variables for Spark
ENV SPARK_HOME=/opt/airflow/.local/lib/python3.9/site-packages/pyspark
ENV PYTHONPATH="${SPARK_HOME}/python:${SPARK_HOME}/python/lib/py4j-0.10.9.7-src.zip:${PYTHONPATH}"
ENV PYSPARK_PYTHON=python3
ENV PYSPARK_DRIVER_PYTHON=python3