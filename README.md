# BD Transformer Airflow Pipeline

This project converts the [BD Transformer Spark package](https://github.com/theend822/BD_Assignment_Spark_Package) into an Airflow workflow pipeline for distributed data transformation.

## Architecture

The pipeline consists of 13 optimized tasks in an end-to-end data workflow:

**Initialization:**
1. **Start Spark Session** - Initialize single shared Spark session for entire pipeline

**Setup (Parallel):**
2. **Create Original Table** - Creates `original_data` table schema
3. **Create Transformed Table** - Creates `transformed_data` table schema  
4. **Create Recovered Table** - Creates `recovered_data` table schema

**Core Pipeline:**
5. **Load Data** - Loads input data using shared Spark session
6. **Fit Transformer** - **Reads from PostgreSQL** (eliminates double parquet read!)
7. **Transform Data** - Applies transformation using the fitted transformer
8. **Inverse Transform** - Recovers original data format

**Database Storage (Parallel):**
9. **Save Original Data** - Stores original data in `original_data` table
10. **Save Transformed Data** - Stores normalized data in `transformed_data` table  
11. **Save Recovered Data** - Stores inverse-transformed data in `recovered_data` table

**Finalization:**
12. **Run DQ Checks** - Validates data quality across all pipeline stages
13. **Stop Spark Session** - Cleanup Spark resources

## Key Features

- **Optimized Resource Usage**: Single Spark session per DAG run (8+ sessions → 1 session!)
- **Eliminated Double Reads**: Transformer reads from PostgreSQL instead of parquet
- **SparkManager**: Centralized Spark session management with error handling
- **PostgreSQL-Spark Integration**: Direct JDBC connectivity for efficient data flow  
- **3-Table Architecture**: Separate tables for original, transformed, and recovered data
- **Configuration-Driven**: SQL schemas and YAML configs externalized as files
- **PythonOperator Integration**: DAG tasks call utility functions instead of custom operators
- **Data Quality Checks**: SQL-based DQ validation after each pipeline stage
- **Docker Containerized**: Complete environment with Airflow, Spark, PostgreSQL, and BD Transformer

## Quick Start

1. **Setup Environment**:
   ```bash
   # Create .env file with database credentials
   cp .env.example .env
   
   # Build and start services
   docker-compose up --build
   ```

2. **Access Airflow**:
   - URL: http://localhost:8080
   - Username: admin
   - Password: admin

3. **Configure Pipeline**:
   - Place input data in `/opt/data/input/`
   - Modify `config/bd_transformer_config.yaml` as needed
   - Trigger the `bd_transformer_pipeline` DAG

4. **View Results**:
   - Check PostgreSQL tables: `bd_customer_profiles_raw`, `bd_customer_profiles_transformed`, `bd_customer_profiles_inverted`
   - Compare data across all 3 tables using `run_id`
   - Validate transformation accuracy by comparing raw vs inverted data

## Configuration

Edit `config/bd_transformer_config.yaml` to customize transformation parameters for each column:

```yaml
column_name:
  converter:
    min_val: 0
    max_val: 100
    clip_oor: true
  normalizer:
    clip: false
    reject: false
```

## Design Choices

- **Single Fit Task**: Uses BD Transformer's native multi-column fitting instead of parallel column tasks
- **Pickle Persistence**: Stores fitted transformer between tasks for efficiency
- **Parallel Database Saves**: Database storage runs in parallel with core pipeline for efficiency
- **Column Prefixing**: Recovered data gets `recovered_` prefix to distinguish from original
- **Local Executor**: Simple deployment suitable for development and small-scale production

## Database Schema

**Three Customer Profile Tables:**

1. **`bd_customer_profiles_raw`**: Raw customer data as loaded from parquet files
   - `run_id`, `day_of_month`, `height`, `account_balance`, `net_profit`, `customer_ratings`, `leaderboard_rank`

2. **`bd_customer_profiles_transformed`**: Normalized/converted customer data (0-1 range)
   - `run_id`, `day_of_month`, `height`, `account_balance`, `net_profit`, `customer_ratings`, `leaderboard_rank`

3. **`bd_customer_profiles_inverted`**: Inverse-transformed customer data (should match raw)
   - `run_id`, `day_of_month`, `height`, `account_balance`, `net_profit`, `customer_ratings`, `leaderboard_rank`

## Data Flow

```
Customer Profile Parquet Files → Spark/Airflow → PostgreSQL Storage

1. /Users/jxy/Downloads/bd_transformer_spark/.../data/*.parquet
   ↓ (Load Data Task)
2. bd_customer_profiles_raw table
   ↓ (Fit Transformer + Transform Data Tasks)  
3. bd_customer_profiles_transformed table  
   ↓ (Inverse Transform Task)
4. bd_customer_profiles_inverted table
   ↓ (DQ Checks)
5. Validation complete
```

## Optimized Data Flow Explanation

### How Data Moves Through the Pipeline (Optimized):

1. **Initialization**: Single Spark session started for entire pipeline

2. **Source Data**: Original parquet files in `/Users/jxy/Downloads/bd_transformer_spark/tests/test_on_large_dataset/data/`
   - Mapped to `/opt/data/input/` in Docker containers

3. **Single Read & Branch**: 
   - `DataLoadOperator` reads customer profile parquet **once** using shared Spark session
   - `PostgresManager.ingest_from_parquet()` saves data to `bd_customer_profiles_raw` table

4. **PostgreSQL-to-Spark Pipeline**:
   - `TransformerFitOperator` **reads from PostgreSQL** (eliminates double parquet read!)
   - Uses **same Spark session** for fitting BD Transformer on customer profiles
   - `TransformOperator` applies transformation, saves to `/opt/data/output/transformed/`

5. **Continued Processing**:
   - `PostgresManager` loads transformed parquet to `bd_customer_profiles_transformed` table
   - `TransformOperator` performs inverse transformation using **shared session**
   - `PostgresManager` loads inverted parquet to `bd_customer_profiles_inverted` table

6. **Quality & Cleanup**:
   - DQ checks validate all 3 customer profile tables using SQL
   - **Spark session stopped** for proper resource cleanup

### Major Optimizations:
- **Resource Efficiency**: 1 Spark session instead of 8+ separate sessions
- **I/O Optimization**: Eliminated duplicate parquet reads via PostgreSQL-Spark integration  
- **Memory Management**: Proper session lifecycle with cleanup
- **Error Resilience**: Centralized error handling through SparkManager

## Architecture Benefits

- **Centralized Data Management**: PostgresManager handles all database connections
- **Modular Functions**: Separate utilities for create_table, ingest_data, run_dq_check  
- **Configuration as Code**: SQL schemas and DQ checks stored as files
- **3-Table Design**: Clear separation of data transformation stages
- **Integrated Workflow**: All steps orchestrated in single Airflow DAG