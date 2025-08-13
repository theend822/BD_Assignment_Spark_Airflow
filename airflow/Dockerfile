# docker/Dockerfile

FROM apache/airflow:2.10.5-python3.9

USER root

# Install Java, git, and wget for Spark
RUN apt-get update && \
    apt-get install -y openjdk-17-jdk git wget && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Download PostgreSQL JDBC driver for Spark (as root)
RUN mkdir -p /opt/spark/jars && \
    wget -O /opt/spark/jars/postgresql-42.6.0.jar \
    https://jdbc.postgresql.org/download/postgresql-42.6.0.jar && \
    chmod -R 755 /opt/spark

USER airflow

# Apple Silicon M4 chip uses arm64 architecture not amd64
ENV JAVA_HOME="/usr/lib/jvm/java-17-openjdk-arm64" 
ENV PATH="${JAVA_HOME}/bin:${PATH}"

# Copy requirements and install Python packages
COPY requirements.txt /opt/airflow/
RUN pip install --no-cache-dir -r /opt/airflow/requirements.txt

# Install bd_transformer package from GitHub
RUN pip install --no-cache-dir git+https://github.com/theend822/BD_Assignment_Spark_Package.git

# Set environment variables for Spark
ENV PYSPARK_PYTHON=python3
ENV PYSPARK_DRIVER_PYTHON=python3