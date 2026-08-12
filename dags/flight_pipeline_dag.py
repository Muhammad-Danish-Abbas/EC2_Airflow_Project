from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from datetime import datetime, timedelta
import requests
import json
import os

API_KEY = os.environ.get("AVIATIONSTACK_API_KEY")
BASE_URL = "http://api.aviationstack.com/v1/flights"
SLACK_WEBHOOK_URL = os.environ.get("SLACK_WEBHOOK_URL")

BRONZE_DIR = "/opt/airflow/data/bronze"


def slack_failure_alert(context):
    """Failure hone par Slack channel mein alert bhejta hai."""
    if not SLACK_WEBHOOK_URL:
        print("SLACK_WEBHOOK_URL not set, skipping alert")
        return

    task_id = context["task_instance"].task_id
    dag_id = context["task_instance"].dag_id
    execution_date = context["ts"]
    log_url = context["task_instance"].log_url

    message = {
        "text": (
            f":red_circle: *Airflow Task Failed*\n"
            f"*DAG:* {dag_id}\n"
            f"*Task:* {task_id}\n"
            f"*Time:* {execution_date}\n"
            f"*Logs:* {log_url}"
        )
    }

    try:
        requests.post(SLACK_WEBHOOK_URL, json=message, timeout=10)
    except Exception as e:
        print(f"Failed to send Slack alert: {e}")


default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": slack_failure_alert,
}


def extract_bronze(**context):
    import boto3

    params = {"access_key": API_KEY, "limit": 20}
    response = requests.get(BASE_URL, params=params, timeout=30)
    response.raise_for_status()
    data = response.json()

    logical_date = context["logical_date"]
    run_date = context["ds"]

    filepath = os.path.join(BRONZE_DIR, f"flights_{run_date}.json")
    with open(filepath, "w") as f:
        json.dump(data, f)
    print(f"Bronze layer saved locally: {filepath}")

    s3_key = (
        f"bronze/year={logical_date.strftime('%Y')}/"
        f"month={logical_date.strftime('%m')}/"
        f"day={logical_date.strftime('%d')}/"
        f"hour={logical_date.strftime('%H')}/"
        f"flights_{context['ts_nodash']}.json"
    )

    s3 = boto3.client("s3")
    s3.upload_file(filepath, "flight-pipeline-bronze-danish", s3_key)
    print(f"Uploaded to S3: s3://flight-pipeline-bronze-danish/{s3_key}")

    # Data quality ke liye record count XCom mein push karo
    record_count = len(data.get("data", []))
    context["ti"].xcom_push(key="record_count", value=record_count)
    print(f"Record count: {record_count}")


def check_data_quality(**context):
    """Agar 0 records aayen to task fail kar do (Slack alert trigger hoga)."""
    record_count = context["ti"].xcom_pull(
        task_ids="extract_bronze", key="record_count"
    )
    print(f"Checking data quality: record_count = {record_count}")

    if record_count is None or record_count == 0:
        raise ValueError(
            f"Data quality check failed: 0 records received from AviationStack API"
        )

    print(f"Data quality check passed: {record_count} records")


SILVER_SQL = """
USE DATABASE FLIGHT_PIPELINE_DB;
USE SCHEMA MEDALLION;

CREATE OR REPLACE TABLE silver_flights AS
SELECT DISTINCT
    f.value:flight:iata::STRING              AS flight_number,
    f.value:airline:name::STRING             AS airline,
    f.value:departure:airport::STRING        AS dep_airport,
    f.value:departure:scheduled::TIMESTAMP_NTZ AS dep_scheduled,
    f.value:arrival:airport::STRING          AS arr_airport,
    f.value:arrival:scheduled::TIMESTAMP_NTZ    AS arr_scheduled,
    f.value:flight_status::STRING            AS flight_status
FROM bronze_flights b,
LATERAL FLATTEN(input => b.raw_data:data) f;
"""

GOLD_SQL = """
USE DATABASE FLIGHT_PIPELINE_DB;
USE SCHEMA MEDALLION;

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
"""


with DAG(
    dag_id="flight_pipeline",
    default_args=default_args,
    description="Flight data pipeline: AviationStack -> S3 (Bronze) -> Snowflake (Silver/Gold)",
    schedule="@daily",
    start_date=datetime(2026, 8, 1),
    catchup=False,
    tags=["flights", "medallion"],
) as dag:

    bronze_task = PythonOperator(
        task_id="extract_bronze",
        python_callable=extract_bronze,
    )

    quality_check_task = PythonOperator(
        task_id="check_data_quality",
        python_callable=check_data_quality,
    )

    silver_task = SQLExecuteQueryOperator(
        task_id="transform_silver",
        conn_id="snowflake_conn",
        sql=SILVER_SQL,
    )

    gold_task = SQLExecuteQueryOperator(
        task_id="load_gold",
        conn_id="snowflake_conn",
        sql=GOLD_SQL,
    )

    bronze_task >> quality_check_task >> silver_task >> gold_task
