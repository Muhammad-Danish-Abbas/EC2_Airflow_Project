-- ============================================
-- Silver Layer Transformation
-- Flattens raw JSON into a clean, deduplicated table
-- ============================================

USE DATABASE flight_pipeline_db;
USE SCHEMA medallion;

CREATE OR REPLACE TABLE silver_flights AS
SELECT DISTINCT
    f.value:flight:iata::STRING                 AS flight_number,
    f.value:airline:name::STRING                AS airline,
    f.value:departure:airport::STRING           AS dep_airport,
    f.value:departure:scheduled::TIMESTAMP_NTZ  AS dep_scheduled,
    f.value:arrival:airport::STRING             AS arr_airport,
    f.value:arrival:scheduled::TIMESTAMP_NTZ    AS arr_scheduled,
    f.value:flight_status::STRING               AS flight_status
FROM bronze_flights b,
LATERAL FLATTEN(input => b.raw_data:data) f;

SELECT * FROM silver_flights LIMIT 10;
