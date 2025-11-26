from airflow import DAG
from airflow.decorators import task
from airflow.utils.dates import days_ago
from datetime import timedelta
import pandas as pd
import os
import random
import json
import requests
from pathlib import Path
import uuid
from typing import Dict, List

import great_expectations as ge

# Import your database models
from database.db import (
    Base,
    SessionLocal,
    engine,
    IngestionStatistic,
)

# -------------------------------------------------------------
# CONFIG
# -------------------------------------------------------------
DATA_DIR = Path("/opt/airflow/Data")
RAW_DATA_SOURCE = DATA_DIR / "raw-data"
LEGACY_RAW_DIR = DATA_DIR / "raw"
GOOD_DIR = DATA_DIR / "good_data"
BAD_DIR = DATA_DIR / "bad_data"
REPORTS_DIR = DATA_DIR / "reports"

DATABASE_URL = "postgresql+psycopg2://admin:admin@db:5432/defence_db"
TEAMS_WEBHOOK = os.environ.get("TEAMS_WEBHOOK", None)

default_args = {
    "owner": "team",
    "depends_on_past": False,
    "retries": 0,
}

# -------------------------------------------------------------
# DAG DEFINITION
# -------------------------------------------------------------
with DAG(
    dag_id="data_ingestion_dag",
    default_args=default_args,
    schedule="*/1 * * * *",  # every minute
    start_date=days_ago(1),
    catchup=False,
    tags=["ingestion"],
):

    # ---------------------------------------------------------
    # 1️⃣ PICK A RANDOM RAW FILE EACH RUN
    # ---------------------------------------------------------
    @task
    def pick_random_raw_file() -> str:
        """Pick a random CSV from raw-data and return its path."""

        RAW_DATA_SOURCE.mkdir(exist_ok=True)

        available_files = list(RAW_DATA_SOURCE.glob("*.csv"))

        if not available_files:
            available_files = list(LEGACY_RAW_DIR.glob("*.csv"))

        if not available_files:
            raise FileNotFoundError(
                f"No CSV files found in {RAW_DATA_SOURCE} or {LEGACY_RAW_DIR}. Populate raw-data before running."
            )

        file_path = random.choice(available_files)
        print(f"Selected RAW file for validation: {file_path}")
        return str(file_path)

    # ---------------------------------------------------------
    # 2️⃣ VALIDATE THE DATA
    # ---------------------------------------------------------
    @task
    def validate_data(file_path: str):
        df = pd.read_csv(file_path)
        ge_df = ge.from_pandas(df)

        errors: List[Dict] = []
        expectation_results: List[Dict] = []
        severity_order = {"low": 1, "medium": 2, "high": 3}
        criticality_level = "low"

        def track_result(name: str, severity: str, description: str, expectation_func):
            nonlocal criticality_level
            try:
                result = expectation_func()
            except Exception as exc:
                result = {"success": False, "exception": str(exc)}

            expectation_results.append(
                {
                    "check": name,
                    "description": description,
                    "severity": severity,
                    "result": result,
                }
            )

            if not result.get("success", False):
                errors.append(
                    {
                        "type": name,
                        "message": description,
                        "criticality": severity,
                        "details": result,
                    }
                )
                if severity_order[severity] > severity_order[criticality_level]:
                    criticality_level = severity

        # 1) Required columns
        required_cols = ["age", "gender", "country", "income"]
        for col in required_cols:
            track_result(
                name="missing_required_column",
                severity="high",
                description=f"Column '{col}' must exist.",
                expectation_func=lambda col=col: ge_df.expect_column_to_exist(col),
            )

        # Only run further checks if required columns exist
        present_cols = all(col in df.columns for col in required_cols)

        if present_cols:
            # 2) Missing values
            for col in required_cols:
                track_result(
                    name="missing_values",
                    severity="medium",
                    description=f"Column '{col}' should not contain nulls.",
                    expectation_func=lambda col=col: ge_df.expect_column_values_to_not_be_null(col),
                )

            # 3) Age numeric
            track_result(
                name="age_not_numeric",
                severity="high",
                description="Age must be numeric (int or float).",
                expectation_func=lambda: ge_df.expect_column_values_to_be_in_type_list(
                    "age", ["int64", "float64", "int32", "float32"]
                ),
            )

            # 4) Age range
            track_result(
                name="age_out_of_range",
                severity="high",
                description="Age must be between 0 and 120.",
                expectation_func=lambda: ge_df.expect_column_values_to_be_between(
                    "age", min_value=0, max_value=120
                ),
            )

            # 5) Gender values
            track_result(
                name="gender_unexpected",
                severity="high",
                description="Gender must be 'male' or 'female'.",
                expectation_func=lambda: ge_df.expect_column_values_to_be_in_set(
                    "gender", ["male", "female"]
                ),
            )

            # 6) Country allowed list
            track_result(
                name="country_unexpected",
                severity="medium",
                description="Country must be one of China, India, or Lebanon.",
                expectation_func=lambda: ge_df.expect_column_values_to_be_in_set(
                    "country", ["China", "India", "Lebanon"]
                ),
            )

            # 7) Income positive and reasonable
            track_result(
                name="income_not_positive",
                severity="high",
                description="Income must be positive and below 1,000,000.",
                expectation_func=lambda: ge_df.expect_column_values_to_be_between(
                    "income", min_value=0, max_value=1_000_000
                ),
            )

            # 8) Duplicate rows
            track_result(
                name="duplicate_rows",
                severity="medium",
                description="Dataset should not contain duplicate rows.",
                expectation_func=lambda: {
                    "success": not df.duplicated().any(),
                    "result": {"duplicate_count": int(df.duplicated().sum())},
                },
            )

        invalid_rows = int(df.isnull().any(axis=1).sum())
        valid_rows = int(len(df) - invalid_rows)

        result = {
            "file_path": file_path,
            "errors": errors,
            "criticality": criticality_level,
            "valid_rows": valid_rows,
            "invalid_rows": invalid_rows,
            "expectations": expectation_results,
        }

        print("Validation:", json.dumps(result, indent=2))
        return result

    # ---------------------------------------------------------
    # 3️⃣ SAVE STATISTICS TO DATABASE (FIXED)
    # ---------------------------------------------------------
    @task
    def save_statistics(validation):
        # Ensure tables exist
        Base.metadata.create_all(bind=engine)

        session = SessionLocal()
        try:
            total_rows = validation["valid_rows"] + validation["invalid_rows"]

            stat = IngestionStatistic(
                file_name=Path(validation["file_path"]).name,
                total_rows=total_rows,
                valid_rows=validation["valid_rows"],
                invalid_rows=validation["invalid_rows"],
                criticality=validation["criticality"],
                report_path=None,  # updated in send_alerts if needed
            )

            session.add(stat)
            session.commit()

            print("✔ Saved ingestion statistics:", {
                "file_name": stat.file_name,
                "total_rows": stat.total_rows,
                "valid_rows": stat.valid_rows,
                "invalid_rows": stat.invalid_rows,
                "criticality": stat.criticality,
            })

        except Exception as e:
            session.rollback()
            print("❌ Error saving ingestion statistics:", e)
            raise
        finally:
            session.close()

    # ---------------------------------------------------------
    # 4️⃣ SEND TEAMS ALERT & GENERATE REPORT
    # ---------------------------------------------------------
    @task
    def send_alerts(validation):
        REPORTS_DIR.mkdir(exist_ok=True)
        report_path = REPORTS_DIR / f"{uuid.uuid4().hex}_report.html"

        error_list_items = "".join(
            f"<li><b>{e['type']}</b> ({e['criticality']}): {e['message']}</li>"
            for e in validation["errors"]
        )
        expectation_rows = "".join(
            f"<tr><td>{res['check']}</td><td>{res['severity']}</td>"
            f"<td>{res['description']}</td><td>{res['result'].get('success')}</td></tr>"
            for res in validation["expectations"]
        )

        html = f"""
        <h1>Data Validation Report</h1>
        <p><b>File:</b> {validation['file_path']}</p>
        <p><b>Criticality:</b> {validation['criticality']}</p>
        <h3>Errors ({len(validation['errors'])})</h3>
        <ul>{error_list_items}</ul>
        <h3>Expectation Results</h3>
        <table border="1" cellpadding="5" cellspacing="0">
            <tr><th>Check</th><th>Severity</th><th>Description</th><th>Success</th></tr>
            {expectation_rows}
        </table>
        """
        report_path.write_text(html)

        summary = (
            f"Criticality: {validation['criticality']} | "
            f"Valid rows: {validation['valid_rows']} | "
            f"Invalid rows: {validation['invalid_rows']} | "
            f"Errors found: {len(validation['errors'])}"
        )

        if TEAMS_WEBHOOK:
            msg = {
                "text": (
                    f"⚠ Data Alert ({validation['criticality']})\n"
                    f"{summary}\n"
                    f"Report: {report_path}"
                )
            }
            requests.post(TEAMS_WEBHOOK, json=msg)

        print(f"Report created: {report_path}")
        return str(report_path)

    # ---------------------------------------------------------
    # 5️⃣ SPLIT INTO GOOD/BAD DATA
    # ---------------------------------------------------------
    @task
    def split_and_save(validation):
        GOOD_DIR.mkdir(exist_ok=True)
        BAD_DIR.mkdir(exist_ok=True)

        df = pd.read_csv(validation["file_path"])
        file_name = Path(validation["file_path"]).name

        good_df = df.dropna()
        bad_df = df[df.isnull().any(axis=1)]

        if bad_df.empty:
            good_df.to_csv(GOOD_DIR / file_name, index=False)
        elif good_df.empty:
            bad_df.to_csv(BAD_DIR / file_name, index=False)
        else:
            good_df.to_csv(GOOD_DIR / file_name, index=False)
            bad_df.to_csv(BAD_DIR / f"BAD_{file_name}", index=False)

        print(f"Preserved RAW file: {validation['file_path']}")

    # ---------------------------------------------------------
    # DAG FLOW
    # ---------------------------------------------------------
    raw_file = pick_random_raw_file()
    validation = validate_data(raw_file)

    save_task = save_statistics(validation)
    alert_task = send_alerts(validation)
    split_task = split_and_save(validation)

    validation >> [save_task, alert_task, split_task]
