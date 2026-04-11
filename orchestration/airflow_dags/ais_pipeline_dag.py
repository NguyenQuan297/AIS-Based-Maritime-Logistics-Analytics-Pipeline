"""
Airflow DAG for the AIS Maritime Logistics Analytics Pipeline.

Schedule: Daily
Tasks:
    1. discover_raw_files - Find new .zst files to ingest
    2. ingest_to_bronze - Convert raw .zst to bronze parquet
    3. bronze_to_silver - Clean and transform to silver layer
    4. build_vessel_metadata - Extract vessel metadata
    5. build_daily_activity - Aggregate daily vessel activity
    6. build_voyage_candidates - Identify voyage segments
    7. build_route_metrics - Compute route statistics
    8. load_postgres - Load gold tables into PostgreSQL
    9. run_quality_checks - Validate output data quality
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

PROJECT_DIR = "/opt/airflow/project"
SPARK_SUBMIT = "spark-submit --master local[*]"

default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "start_date": datetime(2025, 8, 15),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


with DAG(
    dag_id="ais_maritime_pipeline",
    default_args=default_args,
    description="End-to-end AIS data pipeline: raw -> bronze -> silver -> gold -> serving",
    schedule_interval="@daily",
    catchup=False,
    max_active_runs=1,
    tags=["ais", "maritime", "logistics"],
) as dag:

    # Use Airflow execution date as source date
    SOURCE_DATE = "{{ ds }}"

    discover_raw_files = BashOperator(
        task_id="discover_raw_files",
        bash_command=f"ls {PROJECT_DIR}/data/raw/ais/ais-{SOURCE_DATE}.csv.zst",
    )

    ingest_to_bronze = BashOperator(
        task_id="ingest_to_bronze",
        bash_command=(
            f"cd {PROJECT_DIR} && "
            f"{SPARK_SUBMIT} spark_jobs/ingest_raw_to_bronze.py --file-limit 1"
        ),
    )

    bronze_to_silver = BashOperator(
        task_id="bronze_to_silver",
        bash_command=(
            f"cd {PROJECT_DIR} && "
            f"{SPARK_SUBMIT} spark_jobs/bronze_to_silver.py --source-date {SOURCE_DATE}"
        ),
    )

    build_vessel_metadata = BashOperator(
        task_id="build_vessel_metadata",
        bash_command=(
            f"cd {PROJECT_DIR} && "
            f"python -c \""
            f"from spark_jobs.spark_session import get_spark_session; "
            f"from processing.metadata_builder import build_vessel_metadata; "
            f"from config.paths import PathConfig; "
            f"spark = get_spark_session(); "
            f"bronze = spark.read.parquet(str(PathConfig.BRONZE_DIR)); "
            f"build_vessel_metadata(spark, bronze); "
            f"spark.stop()\""
        ),
    )

    build_daily_activity = BashOperator(
        task_id="build_daily_activity",
        bash_command=(
            f"cd {PROJECT_DIR} && "
            f"{SPARK_SUBMIT} spark_jobs/silver_to_gold_activity.py --source-date {SOURCE_DATE}"
        ),
    )

    build_voyage_candidates = BashOperator(
        task_id="build_voyage_candidates",
        bash_command=(
            f"cd {PROJECT_DIR} && "
            f"{SPARK_SUBMIT} spark_jobs/silver_to_gold_voyage.py --source-date {SOURCE_DATE}"
        ),
    )

    build_route_metrics = BashOperator(
        task_id="build_route_metrics",
        bash_command=(
            f"cd {PROJECT_DIR} && "
            f"{SPARK_SUBMIT} spark_jobs/silver_to_gold_routes.py"
        ),
    )

    load_postgres = BashOperator(
        task_id="load_postgres",
        bash_command=(
            f"cd {PROJECT_DIR} && "
            f"python -c \""
            f"from spark_jobs.spark_session import get_spark_session; "
            f"from storage.postgres_loader import PostgresLoader; "
            f"from config.paths import PathConfig; "
            f"spark = get_spark_session(); "
            f"loader = PostgresLoader(); "
            f"loader.load_all_gold_tables("
            f"  activity_df=spark.read.parquet(str(PathConfig.GOLD_ACTIVITY_DIR)), "
            f"  voyages_df=spark.read.parquet(str(PathConfig.GOLD_VOYAGE_DIR)), "
            f"  routes_df=spark.read.parquet(str(PathConfig.GOLD_ROUTE_DIR)), "
            f"  metadata_df=spark.read.parquet(str(PathConfig.GOLD_METADATA_DIR))); "
            f"spark.stop()\""
        ),
    )

    run_quality_checks = BashOperator(
        task_id="run_quality_checks",
        bash_command=(
            f"cd {PROJECT_DIR} && "
            f"python -c \""
            f"from spark_jobs.spark_session import get_spark_session; "
            f"from utils.validation import compute_quality_report; "
            f"from config.paths import PathConfig; "
            f"spark = get_spark_session(); "
            f"silver = spark.read.parquet(str(PathConfig.SILVER_DIR)); "
            f"report = compute_quality_report(silver, 'silver'); "
            f"print(report); "
            f"spark.stop()\""
        ),
    )

    # Task dependencies
    discover_raw_files >> ingest_to_bronze >> bronze_to_silver
    bronze_to_silver >> [build_vessel_metadata, build_daily_activity, build_voyage_candidates]
    build_voyage_candidates >> build_route_metrics
    [build_vessel_metadata, build_daily_activity, build_route_metrics] >> load_postgres
    load_postgres >> run_quality_checks
