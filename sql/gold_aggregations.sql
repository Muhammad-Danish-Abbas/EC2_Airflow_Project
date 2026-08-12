-- ============================================
-- Gold Layer Aggregations
-- Business-ready summary tables for dashboards
-- ============================================

USE DATABASE flight_pipeline_db;
USE SCHEMA medallion;

CREATE OR REPLACE TABLE gold_airline_summary AS
SELECT
    airline,
    COUNT(*) AS total_flights,
    COUNT(DISTINCT dep_airport) AS unique_departure_airports,
    COUNT(DISTINCT arr_airport) AS unique_arrival_airports
FROM silver_flights
WHERE airline IS NOT NULL
GROUP BY airline
ORDER BY total_flights DESC;

CREATE OR REPLACE TABLE gold_status_summary AS
SELECT
    flight_status,
    COUNT(*) AS flight_count
FROM silver_flights
GROUP BY flight_status
ORDER BY flight_count DESC;

SELECT * FROM gold_airline_summary;
SELECT * FROM gold_status_summary;
