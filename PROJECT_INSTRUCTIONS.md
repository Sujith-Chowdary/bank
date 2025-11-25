# Data Science in Production Project Instructions

These instructions describe the requirements for the course project: an ML-powered application built by a team of 3–4 people. The focus is on operationalizing a simple ML use case rather than the modeling effort.

## Components to Build
- **Streamlit UI** for single and batch predictions and for browsing past predictions.
- **FastAPI model service** to serve predictions, persist predictions and features, and expose past predictions.
- **PostgreSQL + SQLAlchemy** database for predictions and data-quality issues.
- **Airflow ingestion job** to ingest data every minute, validate with Great Expectations or TFDV, alert, and organize data.
- **Airflow prediction job** to run every two minutes on newly ingested “good” data using the model API.
- **Grafana dashboards** (two) to monitor data quality/drift with alerts and time-based visuals.

## User Interface Requirements
- **Prediction page**
  - Form for single prediction input.
  - CSV upload for batch predictions (send data, not file paths, to API).
  - Predict button calling the model API; display returned predictions with features.
- **Past predictions page**
  - Date range selector.
  - Source filter: `webapp`, `scheduled predictions`, or `all`.

## Model Service (API)
- Expose endpoints:
  - `predict`: handles single and batch predictions (no file uploads) and saves predictions + features.
  - `past-predictions`: returns saved predictions with features.

## Database Usage
- FastAPI service reads/writes predictions.
- Ingestion job writes data-quality statistics.
- Dashboards read predictions and data-quality issues.

## Data Ingestion Job (Airflow)
1. Randomly read one file from `raw-data` each run (`read-data`).
2. Validate quality with Great Expectations/TFDV, determine criticality (`validate-data`).
3. Save statistics (row counts, issues, filename, etc.) to DB (`save-statistics`).
4. Generate HTML report and alert (e.g., Teams) including criticality, summary, and report link (`send-alerts`).
5. Split/move data: all-good → `good_data`; all-bad → `bad_data`; mixed → split into both (`split-and-save-data`).

Additional guidance:
- Introduce at least seven synthetic data-error types (missing column, missing values, wrong/unknown values, etc.).
- Provide a notebook demonstrating each error type.
- Provide a script to split the main dataset into a configurable number of files for `raw-data`.
- Execute `send-alerts`, `split-and-save-data`, and `save-data-errors` in parallel.

## Prediction Job (Airflow)
- Runs every two minutes.
- Tasks:
  1. `check_for_new_data`: detect new files in `good_data`; if none, mark the entire DAG run as **skipped**.
  2. `make_predictions`: read new files and call the model API for predictions.
- Do not move/rename/delete files in `good_data`; use another mechanism to track new files.

## Monitoring Dashboards (Grafana)
- Build two real-time dashboards:
  1. **Ingested data monitoring**: track data-quality issues (e.g., % invalid rows, errors by category over time).
  2. **Data drift & prediction issues**: monitor serving vs. training drift and model anomalies (e.g., class distribution changes).
- Use thresholds/alerting (green/orange/red), time-based statistics (e.g., last 10–30 minutes), and four distinct charts per dashboard.

Good luck! 🤞 🍀
