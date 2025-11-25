from __future__ import annotations

import json
import os
import random
import sys
from datetime import timedelta
from pathlib import Path
from typing import Dict, List

import pandas as pd
import requests
from airflow import DAG
from airflow.exceptions import AirflowSkipException
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.utils.trigger_rule import TriggerRule
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.append(str(ROOT_DIR))
DATABASE_DIR = ROOT_DIR / "database"
if DATABASE_DIR.exists() and str(DATABASE_DIR) not in sys.path:
    sys.path.append(str(DATABASE_DIR))

from database.db import Base, DataQualityIssue, IngestionStatistic

DATA_DIR = Path("/opt/airflow/Data")
RAW_DIR = DATA_DIR / "raw"
GOOD_DIR = DATA_DIR / "good_data"
BAD_DIR = DATA_DIR / "bad_data"
REPORT_DIR = DATA_DIR / "reports"
VALIDATION_DIR = DATA_DIR / "validation_results"
DATABASE_URL = os.getenv(
    "DATABASE_URL", "postgresql+psycopg2://admin:admin@db:5432/defence_db"
)
TEAMS_WEBHOOK_URL = os.getenv("TEAMS_WEBHOOK_URL")

REQUIRED_COLUMNS = [
    "RowNumber",
    "CustomerId",
    "Surname",
    "CreditScore",
    "Geography",
    "Gender",
    "Age",
    "Tenure",
    "Balance",
    "NumOfProducts",
    "HasCrCard",
    "IsActiveMember",
    "EstimatedSalary",
]
CATEGORICAL_VALUES = {
    "Geography": {"France", "Spain", "Germany"},
    "Gender": {"Male", "Female"},
}
NUMERIC_BOUNDS = {
    "CreditScore": (300, 900),
    "Age": (18, 120),
    "Tenure": (0, 15),
    "Balance": (0, None),
    "NumOfProducts": (1, 4),
    "EstimatedSalary": (0, None),
}


def _load_validation(path: Path) -> Dict:
    with path.open("r", encoding="utf-8") as fp:
        return json.load(fp)


def _persist_validation(path: Path, payload: Dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as fp:
        json.dump(payload, fp, indent=2)


def _detect_errors(df: pd.DataFrame) -> Dict:
    errors: Dict[str, List[int]] = {
        "missing_column": [],
        "missing_value": [],
        "unknown_category": [],
        "out_of_bounds": [],
        "non_numeric": [],
    }
    missing_columns = [col for col in REQUIRED_COLUMNS if col not in df.columns]
    if missing_columns:
        errors["missing_column"] = list(range(len(df)))
        return errors, missing_columns

    for idx, row in df.iterrows():
        for col in REQUIRED_COLUMNS:
            if pd.isna(row[col]):
                errors["missing_value"].append(idx)
        for col, allowed in CATEGORICAL_VALUES.items():
            if col in df.columns and row.get(col) not in allowed:
                errors["unknown_category"].append(idx)
        for col, bounds in NUMERIC_BOUNDS.items():
            if col not in df.columns:
                continue
            try:
                value = float(row[col])
            except Exception:
                errors["non_numeric"].append(idx)
                continue
            lower, upper = bounds
            if (lower is not None and value < lower) or (
                upper is not None and value > upper
            ):
                errors["out_of_bounds"].append(idx)
    return errors, []


def _criticality(total_rows: int, invalid_rows: int, missing_columns: List[str]) -> str:
    if missing_columns or invalid_rows == total_rows:
        return "high"
    ratio = invalid_rows / total_rows if total_rows else 0
    if ratio >= 0.3:
        return "medium"
    return "low"


def read_data(**context):
    if not RAW_DIR.exists():
        raise FileNotFoundError(f"raw-data folder not found: {RAW_DIR}")

    files = list(RAW_DIR.glob("*.csv"))
    if not files:
        raise AirflowSkipException("No raw files to ingest")

    picked_file = random.choice(files)
    context["ti"].xcom_push(key="picked_file", value=str(picked_file))
    return str(picked_file)


def validate_data(**context):
    picked = context["ti"].xcom_pull(key="picked_file")
    if not picked:
        raise AirflowSkipException("No file picked for validation")

    file_path = Path(picked)
    df = pd.read_csv(file_path)
    errors, missing_columns = _detect_errors(df)
    invalid_indices = sorted({idx for rows in errors.values() for idx in rows})
    total_rows = len(df)
    invalid_rows = len(invalid_indices)
    valid_rows = max(total_rows - invalid_rows, 0)
    crit = _criticality(total_rows, invalid_rows, missing_columns)
    errors_by_type = {k: len(set(v)) for k, v in errors.items() if v}

    report_path = REPORT_DIR / f"report_{file_path.stem}.html"
    REPORT_DIR.mkdir(parents=True, exist_ok=True)
    report_html = f"""
    <html><body>
    <h2>Data validation report for {file_path.name}</h2>
    <p><strong>Criticality:</strong> {crit}</p>
    <p><strong>Total rows:</strong> {total_rows} — Valid: {valid_rows} / Invalid: {invalid_rows}</p>
    <h3>Error summary</h3>
    <ul>
        {''.join([f'<li>{err}: {count}</li>' for err, count in errors_by_type.items()]) or '<li>No issues found</li>'}
    </ul>
    <h3>Missing columns</h3>
    <p>{', '.join(missing_columns) if missing_columns else 'None'}</p>
    </body></html>
    """
    report_path.write_text(report_html, encoding="utf-8")

    validation_payload = {
        "file_name": file_path.name,
        "file_path": str(file_path),
        "total_rows": total_rows,
        "valid_rows": valid_rows,
        "invalid_rows": invalid_rows,
        "errors_by_type": errors_by_type,
        "criticality": crit,
        "invalid_indices": invalid_indices,
        "missing_columns": missing_columns,
        "report_path": str(report_path),
    }

    result_path = VALIDATION_DIR / f"validation_{file_path.stem}.json"
    _persist_validation(result_path, validation_payload)
    context["ti"].xcom_push(key="validation_result_path", value=str(result_path))
    return validation_payload


def save_statistics(**context):
    result_path = context["ti"].xcom_pull(key="validation_result_path")
    if not result_path:
        raise AirflowSkipException("No validation results to persist")
    result = _load_validation(Path(result_path))

    engine = create_engine(DATABASE_URL)
    SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False)
    Base.metadata.create_all(bind=engine)
    session = SessionLocal()
    try:
        stat = IngestionStatistic(
            file_name=result["file_name"],
            total_rows=result["total_rows"],
            valid_rows=result["valid_rows"],
            invalid_rows=result["invalid_rows"],
            criticality=result["criticality"],
            report_path=result.get("report_path"),
        )
        session.add(stat)
        session.flush()

        for error_type, count in result.get("errors_by_type", {}).items():
            session.add(
                DataQualityIssue(
                    ingestion_id=stat.id,
                    error_type=error_type,
                    occurrences=count,
                    criticality=result["criticality"],
                )
            )
        session.commit()
    finally:
        session.close()


def send_alerts(**context):
    result_path = context["ti"].xcom_pull(key="validation_result_path")
    if not result_path:
        raise AirflowSkipException("No validation results for alerting")
    result = _load_validation(Path(result_path))
    message = {
        "text": (
            f"Data ingestion validation completed for {result['file_name']} — "
            f"Criticality: {result['criticality']} — "
            f"Invalid rows: {result['invalid_rows']}/{result['total_rows']}. "
            f"Report: {result['report_path']}"
        )
    }
    print(f"Teams alert payload: {message}")
    if TEAMS_WEBHOOK_URL:
        try:
            resp = requests.post(TEAMS_WEBHOOK_URL, json=message, timeout=10)
            resp.raise_for_status()
        except Exception as exc:
            print(f"Failed to send Teams notification: {exc}")


def split_and_save_data(**context):
    result_path = context["ti"].xcom_pull(key="validation_result_path")
    if not result_path:
        raise AirflowSkipException("No validation results to split data")
    result = _load_validation(Path(result_path))
    file_path = Path(result["file_path"])
    if not file_path.exists():
        raise AirflowSkipException(f"File missing for splitting: {file_path}")

    df = pd.read_csv(file_path)
    invalid_indices = set(result.get("invalid_indices", []))
    if result.get("missing_columns"):
        invalid_indices = set(range(len(df)))

    good_rows = [i for i in range(len(df)) if i not in invalid_indices]
    bad_rows = list(invalid_indices)

    GOOD_DIR.mkdir(parents=True, exist_ok=True)
    BAD_DIR.mkdir(parents=True, exist_ok=True)

    if not bad_rows:
        dest = GOOD_DIR / file_path.name
        df.to_csv(dest, index=False)
        print(f"Saved GOOD data → {dest}")
    elif len(bad_rows) == len(df):
        dest = BAD_DIR / file_path.name
        df.to_csv(dest, index=False)
        print(f"Saved BAD data → {dest}")
    else:
        good_df = df.loc[good_rows]
        bad_df = df.loc[bad_rows]
        good_dest = GOOD_DIR / f"GOOD_{file_path.name}"
        bad_dest = BAD_DIR / f"BAD_{file_path.name}"
        good_df.to_csv(good_dest, index=False)
        bad_df.to_csv(bad_dest, index=False)
        print(f"Saved split GOOD data → {good_dest}")
        print(f"Saved split BAD data → {bad_dest}")

    file_path.unlink(missing_ok=True)


def build_dag():
    default_args = {
        "owner": "team",
        "depends_on_past": False,
        "retries": 0,
        "retry_delay": timedelta(minutes=1),
    }

    with DAG(
        dag_id="data_ingestion_dag",
        default_args=default_args,
        schedule_interval="*/1 * * * *",
        start_date=days_ago(1),
        catchup=False,
        tags=["ingestion", "validation"],
    ) as dag:
        read_task = PythonOperator(
            task_id="read_data",
            python_callable=read_data,
            provide_context=True,
        )

        validate_task = PythonOperator(
            task_id="validate_data",
            python_callable=validate_data,
            provide_context=True,
        )

        save_statistics_task = PythonOperator(
            task_id="save_statistics",
            python_callable=save_statistics,
            provide_context=True,
            trigger_rule=TriggerRule.ALL_SUCCESS,
        )

        send_alerts_task = PythonOperator(
            task_id="send_alerts",
            python_callable=send_alerts,
            provide_context=True,
            trigger_rule=TriggerRule.ALL_SUCCESS,
        )

        split_and_save_task = PythonOperator(
            task_id="split_and_save_data",
            python_callable=split_and_save_data,
            provide_context=True,
            trigger_rule=TriggerRule.ALL_SUCCESS,
        )

        read_task >> validate_task >> [save_statistics_task, send_alerts_task, split_and_save_task]

    return dag


dag = build_dag()
