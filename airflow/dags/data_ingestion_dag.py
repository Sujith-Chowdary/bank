from airflow import DAG
from airflow.decorators import task
from airflow.utils.dates import days_ago
from datetime import timedelta
import pandas as pd
import os
import random
import json
import requests
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from pathlib import Path
import uuid
from typing import Dict

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
RAW_DIR = DATA_DIR / "raw"
GOOD_DIR = DATA_DIR / "good_data"
BAD_DIR = DATA_DIR / "bad_data"
REPORTS_DIR = DATA_DIR / "reports"

FULL_DATASET = DATA_DIR / "raw_dataset.csv"

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
    schedule="*/2 * * * *",  # every 2 minutes
    start_date=days_ago(1),
    catchup=False,
    tags=["ingestion"],
):

    # ---------------------------------------------------------
    # 1️⃣ CREATE EXACTLY ONE 20-ROW SPLIT PER RUN
    # ---------------------------------------------------------
    @task
    def create_single_split() -> str:
        RAW_DIR.mkdir(exist_ok=True)

        df = pd.read_csv(FULL_DATASET)
        total_rows = len(df)

        if total_rows < 20:
            raise ValueError("Dataset has fewer than 20 rows!")

        start = random.randint(0, total_rows - 20)
        end = start + 20

        split_df = df.iloc[start:end]
        file_path = RAW_DIR / f"raw_split_{uuid.uuid4().hex}.csv"
        split_df.to_csv(file_path, index=False)

        print(f"Generated 20-row RAW split: {file_path}")
        return str(file_path)

    # ---------------------------------------------------------
    # 2️⃣ VALIDATE THE DATA
    # ---------------------------------------------------------
    @task
    def validate_data(file_path: str):
        df = pd.read_csv(file_path)
        errors = []
        criticality = "low"

        # Required columns
        required_cols = ["age", "gender", "country", "income"]
        missing = [c for c in required_cols if c not in df.columns]
        if missing:
            errors.append(f"Missing required columns: {missing}")
            criticality = "high"

        # Missing values
        if df.isnull().any().any():
            errors.append("Missing values detected.")
            criticality = "medium"

        # Invalid values
        if "age" in df.columns and (df["age"] < 0).any():
            errors.append("Negative age detected.")
            criticality = "high"

        if "gender" in df.columns and not df["gender"].isin(["male", "female"]).all():
            errors.append("Unexpected gender values.")
            criticality = "high"

        if "country" in df.columns:
            allowed = ["China", "India", "Lebanon"]
            if not df["country"].isin(allowed).all():
                errors.append("Unknown country values found.")
                criticality = "medium"

        result = {
            "file_path": file_path,
            "errors": errors,
            "criticality": criticality,
            "valid_rows": int(df.dropna().shape[0]),
            "invalid_rows": int(df.isnull().any(axis=1).sum()),
        }

        print("Validation:", result)
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

        html = f"""
        <h1>Data Validation Report</h1>
        <p><b>File:</b> {validation['file_path']}</p>
        <p><b>Criticality:</b> {validation['criticality']}</p>
        <p><b>Errors:</b></p>
        <ul>
            {''.join(f'<li>{e}</li>' for e in validation['errors'])}
        </ul>
        """
        report_path.write_text(html)

        if TEAMS_WEBHOOK:
            msg = {
                "text": (
                    f"⚠ Data Alert ({validation['criticality']})\n"
                    f"Errors: {validation['errors']}\n"
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

        os.remove(validation["file_path"])
        print(f"Removed RAW file: {validation['file_path']}")

    # ---------------------------------------------------------
    # DAG FLOW
    # ---------------------------------------------------------
    raw_file = create_single_split()
    validation = validate_data(raw_file)

    save_statistics(validation)
    send_alerts(validation)
    split_and_save(validation)
