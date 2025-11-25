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
from pathlib import Path
import uuid


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

# Optional → provide Teams webhook URL via environment variable
TEAMS_WEBHOOK = os.environ.get("TEAMS_WEBHOOK", None)


default_args = {
    "owner": "team",
    "depends_on_past": False,
    "retries": 0,
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



# -------------------------------------------------------------
# DAG DEFINITION
# -------------------------------------------------------------
with DAG(
    dag_id="data_ingestion_dag",
    default_args=default_args,
    schedule="*/2 * * * *",    # every 2 minutes
    start_date=days_ago(1),
    catchup=False,
    tags=["ingestion"],
):

    # ---------------------------------------------------------
    # 1️⃣ CREATE EXACTLY ONE 20-ROW SPLIT FOR THIS DAG RUN
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
    # 2️⃣ VALIDATE THE DATA (required by professor)
    # ---------------------------------------------------------
    @task
    def validate_data(file_path: str):
        df = pd.read_csv(file_path)
        errors = []
        criticality = "low"

        # ---- Rule 1: required columns ----
        required_cols = ["age", "gender", "country", "income"]
        missing = [c for c in required_cols if c not in df.columns]
        if missing:
            errors.append(f"Missing required columns: {missing}")
            criticality = "high"

        # ---- Rule 2: missing values ----
        if df.isnull().any().any():
            errors.append("Missing values detected.")
            criticality = "medium"

        # ---- Rule 3: invalid values ----
        if "age" in df.columns and (df["age"] < 0).any():
            errors.append("Negative age values detected.")
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
    # 3️⃣ SAVE INGESTION STATISTICS TO DATABASE
    # ---------------------------------------------------------
    @task
    def save_statistics(validation):
        engine = create_engine(DATABASE_URL)

        stats = {
            "file_name": Path(validation["file_path"]).name,
            "nb_rows": int(validation["valid_rows"] + validation["invalid_rows"]),
            "nb_valid_rows": int(validation["valid_rows"]),
            "nb_invalid_rows": int(validation["invalid_rows"]),
            "criticality": validation["criticality"],
            "errors_json": json.dumps(validation["errors"]),
        }

        with engine.begin() as conn:
            conn.execute(
                text("""
                    INSERT INTO ingestion_statistics
                    (file_name, nb_rows, nb_valid_rows, nb_invalid_rows, criticality, errors_json)
                    VALUES (:file_name, :nb_rows, :nb_valid_rows, :nb_invalid_rows, :criticality, :errors_json)
                """),
                stats,
            )

        print("Saved statistics:", stats)

    # ---------------------------------------------------------
    # 4️⃣ SEND TEAMS ALERT & GENERATE HTML REPORT
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

        # Optional Teams alert
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
    # 5️⃣ SPLIT INTO GOOD/BAD DATA AND SAVE TO FOLDERS
    # ---------------------------------------------------------
    @task
    def split_and_save(validation):
        GOOD_DIR.mkdir(exist_ok=True)
        BAD_DIR.mkdir(exist_ok=True)

        df = pd.read_csv(validation["file_path"])
        file_name = Path(validation["file_path"]).name


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

    # Run these 3 tasks in parallel
    save_statistics(validation)
    send_alerts(validation)
    split_and_save(validation)
