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

import great_expectations as ge
from great_expectations.render.renderer import ValidationResultsPageRenderer
from great_expectations.render.view import DefaultJinjaPageView

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.append(str(ROOT_DIR))
DATABASE_DIR = ROOT_DIR / "database"
if DATABASE_DIR.exists() and str(DATABASE_DIR) not in sys.path:
    sys.path.append(str(DATABASE_DIR))

from database.db import Base, DataQualityIssue, IngestionStatistic

DAGS_ROOT = Path(__file__).resolve().parent
DATA_DIR = Path(os.getenv("DATA_DIR", "/opt/airflow/Data"))
RAW_DIR = Path(os.getenv("RAW_DATA_DIR", DATA_DIR / "raw"))
FALLBACK_RAW_DIR = Path(os.getenv("FALLBACK_RAW_DIR", DATA_DIR))
# Additional candidate locations to catch common mount patterns (/opt/airflow/dags/Data, repo Data)
EXTRA_RAW_DIRS = [
    DAGS_ROOT / "Data",
    ROOT_DIR / "Data",
    ROOT_DIR / "airflow" / "Data",
]
GOOD_DIR = Path(os.getenv("GOOD_DATA_DIR", DATA_DIR / "good_data"))
BAD_DIR = Path(os.getenv("BAD_DATA_DIR", DATA_DIR / "bad_data"))
REPORT_DIR = Path(os.getenv("REPORT_DIR", DATA_DIR / "reports"))
VALIDATION_DIR = Path(
    os.getenv("VALIDATION_DIR", DATA_DIR / "validation_results")
)
try:
    _raw_chunk = int(os.getenv("INGEST_SPLIT_CHUNK_SIZE", "20"))
except ValueError:
    _raw_chunk = 20

SPLIT_CHUNK_SIZE = max(10, min(20, _raw_chunk))
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
        "invalid_boolean": [],
        "duplicate_row": [],
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
        for bool_col in ["HasCrCard", "IsActiveMember"]:
            if bool_col in df.columns and row.get(bool_col) not in {0, 1, "0", "1"}:
                errors["invalid_boolean"].append(idx)

    if not df.empty:
        dup_indices = df[df.duplicated()].index.tolist()
        errors["duplicate_row"].extend(dup_indices)
    return errors, []


def _criticality(total_rows: int, invalid_rows: int, missing_columns: List[str]) -> str:
    if missing_columns or invalid_rows == total_rows:
        return "high"
    ratio = invalid_rows / total_rows if total_rows else 0
    if ratio >= 0.3:
        return "medium"
    return "low"


def read_data(**context):
    search_paths = [RAW_DIR, FALLBACK_RAW_DIR] + [p for p in EXTRA_RAW_DIRS if p]
    discovered = []

    for path in search_paths:
        if not path.exists():
            continue
        candidates = list(path.glob("*.csv"))
        print(f"Scanned {path.resolve()} -> {len(candidates)} file(s)")
        discovered.extend(candidates)

    if not discovered:
        raise AirflowSkipException(
            "No raw files to ingest. Stage .csv files under RAW_DATA_DIR, FALLBACK_RAW_DIR, or Data next to dags."
        )

    picked_file = random.choice(discovered)
    context["ti"].xcom_push(key="picked_file", value=str(picked_file))
    return str(picked_file)


def validate_data(**context):
    picked = context["ti"].xcom_pull(key="picked_file")
    if not picked:
        raise AirflowSkipException("No file picked for validation")

    file_path = Path(picked)
    df = pd.read_csv(file_path)
    validator = ge.from_pandas(df)
    validator.expect_table_columns_to_match_set(REQUIRED_COLUMNS)
    for col in REQUIRED_COLUMNS:
        validator.expect_column_values_to_not_be_null(col)
    for col, allowed in CATEGORICAL_VALUES.items():
        validator.expect_column_values_to_be_in_set(col, allowed)
    for col, (lower, upper) in NUMERIC_BOUNDS.items():
        validator.expect_column_values_to_be_between(
            col,
            min_value=lower,
            max_value=upper,
            allow_cross_type_comparisons=True,
        )
    for bool_col in ["HasCrCard", "IsActiveMember"]:
        validator.expect_column_values_to_be_in_set(bool_col, {0, 1})

    ge_result = validator.validate(result_format="COMPLETE")
    errors, missing_columns = _detect_errors(df)
    invalid_indices = sorted({idx for rows in errors.values() for idx in rows})
    total_rows = len(df)
    invalid_rows = len(invalid_indices)
    valid_rows = max(total_rows - invalid_rows, 0)
    crit = _criticality(total_rows, invalid_rows, missing_columns)
    errors_by_type = {k: len(set(v)) for k, v in errors.items() if v}

    report_path = REPORT_DIR / f"report_{file_path.stem}.html"
    REPORT_DIR.mkdir(parents=True, exist_ok=True)
    renderer = ValidationResultsPageRenderer()
    rendered_document = renderer.render(ge_result)
    html = DefaultJinjaPageView().render(rendered_document)
    report_path.write_text(html, encoding="utf-8")

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
        "ge_success": bool(getattr(ge_result, "success", False)),
        "ge_statistics": getattr(ge_result, "statistics", {}),
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
    report_link = result.get("report_path", "")
    message = {
        "text": (
            f"Data ingestion validation completed for {result['file_name']} — "
            f"Criticality: {result['criticality']} — "
            f"Invalid rows: {result['invalid_rows']}/{result['total_rows']}. "
            f"[View report]({report_link})"
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

    def _write_chunks(data: pd.DataFrame, dest_dir: Path, prefix: str) -> None:
        if data.empty:
            return
        base_name = file_path.stem
        for i, start in enumerate(range(0, len(data), SPLIT_CHUNK_SIZE), start=1):
            chunk = data.iloc[start : start + SPLIT_CHUNK_SIZE]
            dest = dest_dir / f"{prefix}{base_name}_part{i}.csv"
            chunk.to_csv(dest, index=False)
            print(f"Saved {prefix.rstrip('_')} chunk with {len(chunk)} rows → {dest}")

    if not bad_rows:
        _write_chunks(df, GOOD_DIR, "GOOD_")
    elif len(bad_rows) == len(df):
        _write_chunks(df, BAD_DIR, "BAD_")
    else:
        good_df = df.loc[good_rows]
        bad_df = df.loc[bad_rows]
        _write_chunks(good_df, GOOD_DIR, "GOOD_")
        _write_chunks(bad_df, BAD_DIR, "BAD_")

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
