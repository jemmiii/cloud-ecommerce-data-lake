from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

from pathlib import Path
import subprocess
import sys


PROJECT_ROOT = Path(__file__).resolve().parents[1]


def run_pipeline():
    commands = [
        [
            sys.executable,
            str(PROJECT_ROOT / "src" / "ingestion" / "ingest_raw.py"),
        ],
        [
            sys.executable,
            str(PROJECT_ROOT / "src" / "transformation" / "silver_transform.py"),
        ],
        [
            sys.executable,
            str(PROJECT_ROOT / "src" / "analytics" / "gold_analytics.py"),
        ],
    ]

    for command in commands:
        subprocess.run(command, cwd=PROJECT_ROOT, check=True)


with DAG(
    dag_id="cloud_ecommerce_data_lake",
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["pyspark", "data-engineering", "etl"],
) as dag:

    run_etl_pipeline = PythonOperator(
        task_id="run_etl_pipeline",
        python_callable=run_pipeline,
    )