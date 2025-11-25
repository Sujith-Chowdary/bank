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
from airflow.utils.state import DagRunState
from sqlalchemy import create_engine

GOOD_DIR = Path("/opt/airflow/Data/good_data")
FASTAPI_URL = os.environ.get("FASTAPI_URL", "http://fastapi:8000/predict")
DATABASE_URL = os.environ.get(
    "DATABASE_URL", "postgresql+psycopg2://admin:admin@db:5432/defence_db"
)
CHUNK_SIZE = 500


def _get_processed_files():
    engine = create_engine(DATABASE_URL)
    try:
        df = pd.read_sql("SELECT DISTINCT source_file FROM predictions", engine)
        return set(df["source_file"].dropna().tolist())
    except Exception:
        return set()


def check_for_new_data(**context):
    dag_run = context["dag_run"]
    files = [f for f in GOOD_DIR.glob("*.csv")]
    processed_files = _get_processed_files()
    new_files = [f.name for f in files if f.name not in processed_files]

    if not new_files:
        dag_run.set_state(DagRunState.SKIPPED)
        raise AirflowSkipException("No new files found — skipping prediction run")

    context["ti"].xcom_push(key="new_files", value=new_files)
    return new_files


def make_predictions(**context):
    files = context["ti"].xcom_pull(key="new_files")
    if not files:
        raise AirflowSkipException("No files to predict")

    for file in files:
        file_path = GOOD_DIR / file
        if not file_path.exists():
            print(f"File missing, skipping: {file_path}")
            continue

        for chunk in pd.read_csv(file_path, chunksize=CHUNK_SIZE):
            payload = chunk.to_dict(orient="records")
            try:
                response = requests.post(
                    FASTAPI_URL,
                    json=payload,
                    params={"source": "scheduled", "source_file": file},
                    timeout=60,
                )
                response.raise_for_status()
            except Exception as exc:
                print(f"Error calling FastAPI for {file}: {exc}")
                continue

            result = response.json()
            if isinstance(result, dict) and "predictions" in result:
                predictions = result["predictions"]
            elif isinstance(result, list):
                predictions = result
            else:
                predictions = [result]
            print(
                f"Processed chunk of {len(chunk)} rows from {file} — received {len(predictions)} predictions"
            )


def build_dag():
    default_args = {
        "owner": "airflow",
        "depends_on_past": False,
        "retries": 0,
        "retry_delay": timedelta(minutes=1),
    }

    with DAG(
        "prediction_dag",
        default_args=default_args,
        description="Prediction DAG with dagrun skip handling",
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
