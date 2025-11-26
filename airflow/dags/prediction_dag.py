from __future__ import annotations

import os
import json
from datetime import timedelta
from pathlib import Path

import pandas as pd
import requests
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.python import ShortCircuitOperator
from airflow.utils.dates import days_ago
from sqlalchemy import create_engine


# ------------ CONFIG ------------
GOOD_DIR = Path("/opt/airflow/Data/good_data")
STATE_FILE = Path("/opt/airflow/Data/processed_files_state.json")

FASTAPI_URL = os.environ.get(
    "FASTAPI_URL",
    "http://fastapi:8000/predict"
)

DATABASE_URL = os.environ.get(
    "DATABASE_URL",
    "postgresql+psycopg2://admin:admin@db:5432/defence_db"
)

def _load_state_file():
    if not STATE_FILE.exists():
        return {}

    try:
        with STATE_FILE.open("r") as f:
            data = json.load(f)
            return data.get("files", {})
    except Exception:
        return {}


def _persist_state_file(processed_files):
    state_data = _load_state_file()
    state_data.update(processed_files)
    STATE_FILE.parent.mkdir(parents=True, exist_ok=True)

    with STATE_FILE.open("w") as f:
        json.dump({"files": state_data}, f, indent=2)


# ----------- Helper: Get processed files ------------
def _get_processed_files():
    processed = set()
    engine = create_engine(DATABASE_URL)
    try:
        df = pd.read_sql("SELECT DISTINCT source_file FROM predictions", engine)
        processed.update(df["source_file"].dropna().tolist())
    except Exception:
        # Gracefully fall back to state file only
        pass

    # Merge with state file records to ensure we respect timestamp-based processing
    state_data = _load_state_file()
    processed.update(state_data.keys())
    return processed, state_data


# ------------ Task 1: Check for New Data ------------
def short_circuit_on_new_data(**context):
    """Skip the DAG run when there are no new/updated files in good_data."""

    processed, state_data = _get_processed_files()
    new_files = []

    for file_path in GOOD_DIR.glob("*.csv"):
        modified = int(file_path.stat().st_mtime)
        state_mtime = state_data.get(file_path.name)

        already_processed = file_path.name in processed and state_mtime is not None and modified <= state_mtime

        if not already_processed:
            new_files.append({"name": file_path.name, "mtime": modified})

    if not new_files:
        return False

    context["ti"].xcom_push(key="new_files", value=new_files)
    return True


def make_predictions(**context):
    new_files = context["ti"].xcom_pull(key="new_files") or []
    if not new_files:
        # Upstream short-circuit already skipped downstream tasks
        return

    aggregated_payload = []
    processed_state = {}

    for file_info in new_files:
        file_name = file_info.get("name") if isinstance(file_info, dict) else file_info
        file_mtime = file_info.get("mtime") if isinstance(file_info, dict) else None

        file_path = GOOD_DIR / file_name

        if not file_path.exists():
            print(f"Missing file: {file_path}")
            continue

        try:
            df = pd.read_csv(file_path)
            df["source_file"] = file_name
            aggregated_payload.extend(df.to_dict(orient="records"))
            processed_state[file_name] = file_mtime or int(file_path.stat().st_mtime)
        except Exception as exc:
            print(f"Failed reading {file_path}: {exc}")

    if not aggregated_payload:
        return

    try:
        response = requests.post(
            FASTAPI_URL,
            json=aggregated_payload,
            params={"source": "scheduled predictions"},
            timeout=60,
        )
        response.raise_for_status()

        preds = response.json()
        print(f"Sent {len(aggregated_payload)} rows for prediction across {len(new_files)} files.")

        # Persist state only after successful API call
        _persist_state_file(processed_state)
    except Exception as exc:
        print(f"Prediction batch failed: {exc}")


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

        check_task = ShortCircuitOperator(
            task_id="short_circuit_on_new_data",
            python_callable=short_circuit_on_new_data,
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
