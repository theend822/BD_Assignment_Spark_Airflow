# BD Transformer Airflow Pipeline

This project converts the [BD Transformer Spark package](https://github.com/theend822/BD_Assignment_Spark_Package) into an Airflow workflow pipeline for distributed data transformation.

## Architecture

The pipeline consists of 10 optimized tasks in an end-to-end data workflow:

1. **Create Raw Table** - Creates `bd_customer_profiles_raw` table schema
2. **Load Raw Data** - Loads parquet data into PostgreSQL raw table using Spark
3. **Create Transformed Table** - Creates `bd_customer_profiles_transformed` table schema
4. **Create Inverted Table** - Creates `bd_customer_profiles_inverted` table schema
5. **Start Spark Session** - Initialize shared Spark session for pipeline
6. **Fit Transform** - Reads from PostgreSQL, fits BD transformer, saves transformed data
7. **Inverse Transform** - Reads transformed data, applies inverse transformation
8. **DQ Check Transformed** - Validates null values in transformed data
9. **DQ Check Inverted** - Validates null values in inverted data  
10. **DQ Check Row Counts** - Ensures row count consistency across all tables
11. **Stop Spark Session** - Cleanup Spark resources

## Data Flow

```
Customer Profile Parquet Files → Spark/Airflow → PostgreSQL Storage

1. /Users/jxy/Downloads/bd_transformer_spark/.../data/*.parquet
   ↓ (Load Raw Data Task)
2. bd_customer_profiles_raw table
   ↓ (Fit Transform Task)  
3. bd_customer_profiles_transformed table  
   ↓ (Inverse Transform Task)
4. bd_customer_profiles_inverted table
   ↓ (DQ Checks)
5. Validation complete
```

**Key Optimizations:**
- **Single Spark Session**: Shared across all transformation tasks
- **Direct PostgreSQL Integration**: Eliminates intermediate file operations
- **Memory Efficient**: Processes data in streaming fashion
- **JDBC Connectivity**: Direct Spark-to-PostgreSQL data transfer

## Key Features

- **Optimized Resource Usage**: Single Spark session per DAG run
- **SparkManager / PostgresManager**: Centralized management for integration
- **PostgreSQL-Spark Integration**: Direct JDBC connectivity for efficient data flow  
- **Config-Driven**: Leverage YAML config to avoid DRY
- **Data Quality Checks**: Automated null value and row count validation
- **Docker Containerized**: Complete environment with Airflow, Spark, PostgreSQL, and BD Transformer

## Further Improvements

### a. System Environment Issues and Current Temporary Fix

**Issue**: Environment variables (POSTGRES_HOST, POSTGRES_USER, etc.) are not available in custom operator execution contexts, causing PostgreSQL connection failures.

**Current Fix**: Hardcoded database credentials passed directly through DAG task parameters:
```python
postgres_config={
    'host': 'postgres',
    'port': '5432', 
    'database': 'bd_datamart',
    'user': 'theend822',
    'password': '...',
}
```

**Recommended Solution**: Look into this issue further and find the root cause. May just need to simply remove the cache.

### b. PostgreSQL Writes via JDBC Are Very Slow

**Issue**: Writing large datasets (8GB) to PostgreSQL via Spark JDBC is inherently slow, taking 5+ minutes for bulk operations. But running via pure spark only took <20 seconds. 

**Optimization Opportunities**:
- Use PostgreSQL COPY commands instead of JDBC INSERT statements
- Implement bulk loading with intermediate CSV/Parquet staging
- Configure JDBC batch size and connection pooling
- Use PostgreSQL-specific Spark connectors for better performance

### c. Fit Transform Task Crashes Due to Disk I/O Overload

**Issue**: The BD Transformer package performs multiple full dataset scans during fitting (calculating min/max statistics for each column), causing disk I/O to reach 110%+ capacity and leading to JVM crashes.

**Evidence**:
![Disk I/O at 110% before crash](https://drive.google.com/file/d/1vQmD5MVtXqbM-lVu789ys6iFCmW938yt/view?usp=drive_link)

![JVM crash error from I/O overload](https://drive.google.com/file/d/1_WiR4ZcksgROZTiD0aDNB1x1cLJXbmHz/view?usp=drive_link)

**Root Cause**: BD Transformer scans the 8GB dataset 6 times (once per column) for statistics collection, overwhelming Docker's I/O capacity.

**Optimization Recommendations**:
- Modify BD Transformer to collect all column statistics in a single dataset scan
- Implement DataFrame caching and persistence strategies
- Use smaller dataset chunks for development and testing
- Consider distributed processing across multiple nodes for production workloads
