-- ============================================
-- Bronze Layer Setup
-- Storage integration, external stage, raw table
-- ============================================

USE DATABASE flight_pipeline_db;
USE SCHEMA medallion;

-- External stage: Snowflake's "window" into the S3 bronze landing zone
CREATE STAGE IF NOT EXISTS bronze_stage
  URL = 's3://<your-bucket-name>/bronze/'
  STORAGE_INTEGRATION = S3_FLIGHT_PIPELINE_INTEGRATION
  FILE_FORMAT = (TYPE = 'JSON');

-- Confirm the stage can see files landed by Airflow
LIST @bronze_stage;

-- Bronze table: raw JSON kept as-is in VARIANT format
CREATE TABLE IF NOT EXISTS bronze_flights (
    raw_data VARIANT,
    file_name STRING,
    loaded_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Load new files from the stage into the Bronze table
COPY INTO bronze_flights (raw_data, file_name)
FROM (
    SELECT $1, METADATA$FILENAME
    FROM @bronze_stage
)
FILE_FORMAT = (TYPE = 'JSON')
ON_ERROR = 'CONTINUE';

SELECT * FROM bronze_flights LIMIT 5;
