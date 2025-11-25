import os
from datetime import datetime, time

import pandas as pd
import requests
import streamlit as st

API_BASE_URL = os.getenv("FASTAPI_URL", "http://localhost:8000")
API_URL = f"{API_BASE_URL}/predict"
PAST_PREDICTIONS_URL = f"{API_BASE_URL}/past-predictions"

st.set_page_config(page_title="Churn Prediction", layout="wide")
st.title("Churn Prediction Webapp")

menu = ["Single Prediction", "Batch Prediction", "Past Predictions"]
choice = st.sidebar.selectbox("Menu", menu)

if choice == "Single Prediction":
    st.header("Make a Single Prediction")

    with st.form(key="single_form"):
        CreditScore = st.number_input("Credit Score", min_value=300, max_value=900, value=600)
        Geography = st.selectbox("Geography", ["France", "Spain", "Germany"])
        Gender = st.selectbox("Gender", ["Male", "Female"])
        Age = st.number_input("Age", min_value=18, max_value=100, value=40)
        Tenure = st.number_input("Tenure", min_value=0, max_value=10, value=3)
        Balance = st.number_input("Balance", min_value=0.0, value=60000.0)
        NumOfProducts = st.number_input("Number of Products", min_value=1, max_value=4, value=2)
        HasCrCard = st.selectbox("Has Credit Card", [0, 1], index=1)
        IsActiveMember = st.selectbox("Is Active Member", [0, 1], index=1)
        EstimatedSalary = st.number_input("Estimated Salary", min_value=0.0, value=50000.0)

        submit_button = st.form_submit_button("Predict")

    if submit_button:
        payload = {
            "CreditScore": CreditScore,
            "Geography": Geography,
            "Gender": Gender,
            "Age": Age,
            "Tenure": Tenure,
            "Balance": Balance,
            "NumOfProducts": NumOfProducts,
            "HasCrCard": HasCrCard,
            "IsActiveMember": IsActiveMember,
            "EstimatedSalary": EstimatedSalary,
        }

        try:
            response = requests.post(API_URL, json=payload, params={"source": "webapp"})
            response.raise_for_status()
            result = response.json()

            if "prediction_label" in result:
                if result.get("prediction") == 1:
                    st.error(f"🚨 {result['prediction_label']}")
                else:
                    st.success(f"✅ {result['prediction_label']}")
            else:
                st.info(f"Prediction: {result.get('prediction')}")

        except Exception as e:
            st.error(f"API call failed: {e}")

elif choice == "Batch Prediction":
    st.header("Upload CSV for Batch Predictions")
    uploaded_file = st.file_uploader("Choose a CSV file", type="csv")
    if uploaded_file is not None:
        df = pd.read_csv(uploaded_file)
        st.write("Uploaded Data:")
        st.dataframe(df)

        if st.button("Predict All"):
            try:
                payload = df.to_dict(orient="records")
                response = requests.post(
                    API_URL,
                    json=payload,
                    params={"source": "webapp", "source_file": "webapp_upload"},
                )
                response.raise_for_status()
                results = response.json()

                if "predictions" in results:
                    prediction_values = [item.get("prediction") for item in results["predictions"]]
                    labels = [item.get("prediction_label", "") for item in results["predictions"]]
                else:
                    prediction_values = [results.get("prediction")]
                    labels = [results.get("prediction_label", "")]

                df["Prediction"] = labels
                df["RawPrediction"] = prediction_values
                st.success("Predictions Added:")
                st.dataframe(df)

            except Exception as e:
                st.error(f"API call failed: {e}")

elif choice == "Past Predictions":
    st.header("View Past Predictions from DB")

    source_filter = st.selectbox(
        "Prediction source", ["all", "webapp", "scheduled"], index=0
    )
    use_date_filter = st.checkbox("Filter by date range", value=False)

    params = {"source": source_filter}

    if use_date_filter:
        start_date = st.date_input("Start date", value=datetime.utcnow().date())
        end_date = st.date_input("End date", value=datetime.utcnow().date())

        start_dt = datetime.combine(start_date, time.min)
        end_dt = datetime.combine(end_date, time.max)

        params["start_date"] = start_dt.isoformat()
        params["end_date"] = end_dt.isoformat()

    if st.button("Fetch Past Predictions"):
        try:
            response = requests.get(PAST_PREDICTIONS_URL, params=params)
            response.raise_for_status()
            data = response.json()

            if "past_predictions" in data and data["past_predictions"]:
                df = pd.DataFrame(data["past_predictions"])
                st.dataframe(df)
            else:
                st.warning("No past predictions found for the selected filters.")
        except Exception as e:
            st.error(f"Failed to fetch data: {e}")
