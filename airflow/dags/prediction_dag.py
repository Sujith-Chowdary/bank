from __future__ import annotations

import os
from datetime import timedelta
from pathlib import Path

import pandas as pd
import requests
from airflow import DAG
from airflow.exceptions import AirflowSkipException
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from sqlalchemy import create_engine


# ------------ CONFIG ------------
GOOD_DIR = Path("/opt/airflow/Data/good_data")

FASTAPI_URL = os.environ.get(
    "FASTAPI_URL",
    "http://fastapi:8000/predict"
)

DATABASE_URL = os.environ.get(
    "DATABASE_URL",
    "postgresql+psycopg2://admin:admin@db:5432/defence_db"
)

CHUNK_SIZE = 500


# ----------- Helper: Get processed files ------------
def _get_processed_files():
    engine = create_engine(DATABASE_URL)
    try:
        df = pd.read_sql("SELECT DISTINCT source_file FROM predictions", engine)
        return set(df["source_file"].dropna().tolist())
    except Exception:
        return set()


# ------------ Task 1: Check for New Data ------------
def check_for_new_data(**context):
    """
    REQUIREMENT:
    - If no new files → mark *entire DAG run* as SKIPPED.
    """
    all_files = [f.name for f in GOOD_DIR.glob("*.csv")]
    processed = _get_processed_files()

    new_files = [f for f in all_files if f not in processed]

    if not new_files:
        # Skip ENTIRE DAG RUN
        raise AirflowSkipException("No new files → skipping full DAG run.")

    # Send new files list to next task
    context["ti"].xcom_push(key="new_files", value=new_files)
    return new_files


# ------------ Task 2: Make Predictions ------------
def make_predictions(**context):
    new_files = context["ti"].xcom_pull(key="new_files")
    if not new_files:
        raise AirflowSkipException("No files to process.")

    engine = create_engine(DATABASE_URL)

    for file in new_files:
        file_path = GOOD_DIR / file

        if not file_path.exists():
            print(f"Missing file: {file_path}")
            continue

        # Read file in chunks
        for chunk in pd.read_csv(file_path, chunksize=CHUNK_SIZE):
            payload = chunk.to_dict(orient="records")

            try:
                response = requests.post(
                    FASTAPI_URL,
                    json=payload,
                    params={"source": "scheduled", "source_file": file},
                    timeout=30,
                )
                response.raise_for_status()

                preds = response.json()
                print(f"Processed {len(chunk)} rows for file: {file}")

            except Exception as exc:
                print(f"Prediction failed for {file}: {exc}")


# ------------ DAG Definition ------------
def build_dag():
    default_args = {"owner": "airflow", "retries": 0}

    with DAG(
        "prediction_dag",
        default_args=default_args,
        description="Scheduled prediction pipeline",
        schedule_interval=timedelta(minutes=2),
        start_date=days_ago(1),
        catchup=False,
        tags=["prediction"],
    ) as dag:

        check_task = PythonOperator(
            task_id="check_for_new_data",
            python_callable=check_for_new_data,
            provide_context=True,
        )

        predict_task = PythonOperator(
            task_id="make_predictions",
            python_callable=make_predictions,
            provide_context=True,
        )

        check_task >> predict_task

    return dag


dag = build_dag()