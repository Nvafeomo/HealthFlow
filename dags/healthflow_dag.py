import os
import time
from datetime import datetime, timedelta

import requests
from dotenv import load_dotenv

from airflow import DAG
from airflow.operators.python import PythonOperator

load_dotenv()

DATABRICKS_HOST = os.environ.get("DATABRICKS_HOST", "").rstrip("/")
DATABRICKS_TOKEN = os.environ.get("DATABRICKS_TOKEN", "")
DATABRICKS_JOB_ID = os.environ.get("DATABRICKS_JOB_ID", "")  # fill in after job creation

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


# ── Task 1: Extract ──────────────────────────────────────────────────────────

def run_extract(**context):
    from scripts.extract import extract
    token = os.environ.get("SOCRATA_APP_TOKEN", "")
    output_path = extract(app_token=token)
    # Push the output path so the next task can find it
    context["ti"].xcom_push(key="raw_path", value=output_path)


# ── Task 2: Upload raw CSV to Databricks Volume, trigger job, download clean ─

def run_transform(**context):
    from scripts.dbfs_utils import upload_to_volume, download_from_volume

    raw_path = context["ti"].xcom_pull(key="raw_path", task_ids="extract")
    token = DATABRICKS_TOKEN

    # Upload raw file to Volume
    upload_to_volume(raw_path, "raw_cdc.csv", token)
    print("Uploaded raw_cdc.csv to Volume")

    # Trigger the Databricks job
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }
    run_resp = requests.post(
        f"{DATABRICKS_HOST}/api/2.1/jobs/run-now",
        headers=headers,
        json={"job_id": int(DATABRICKS_JOB_ID)},
    )
    run_resp.raise_for_status()
    run_id = run_resp.json()["run_id"]
    print(f"Triggered Databricks job run_id={run_id}")

    # Poll until the job finishes
    while True:
        time.sleep(15)
        status_resp = requests.get(
            f"{DATABRICKS_HOST}/api/2.1/jobs/runs/get?run_id={run_id}",
            headers=headers,
        )
        status_resp.raise_for_status()
        state = status_resp.json()["state"]
        life_cycle = state["life_cycle_state"]
        print(f"  Job state: {life_cycle}")

        if life_cycle in ("TERMINATED", "SKIPPED", "INTERNAL_ERROR"):
            result = state.get("result_state", "UNKNOWN")
            if result != "SUCCESS":
                raise RuntimeError(f"Databricks job failed: result_state={result}")
            break

    # Download the clean CSV back
    clean_path = "data/staging/clean_cdc.csv"
    download_from_volume("clean_cdc.csv", clean_path, token)
    context["ti"].xcom_push(key="clean_path", value=clean_path)
    print(f"Downloaded clean CSV to {clean_path}")


# ── Task 3: Load ─────────────────────────────────────────────────────────────

def run_load(**context):
    from scripts.load import load
    clean_path = context["ti"].xcom_pull(key="clean_path", task_ids="transform")
    load(clean_path)


# ── DAG definition ───────────────────────────────────────────────────────────

with DAG(
    dag_id="healthflow_pipeline",
    default_args=default_args,
    description="Weekly CDC deaths ETL: extract → Databricks transform → Postgres load",
    schedule_interval="@weekly",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["healthflow"],
) as dag:

    extract_task = PythonOperator(
        task_id="extract",
        python_callable=run_extract,
    )

    transform_task = PythonOperator(
        task_id="transform",
        python_callable=run_transform,
    )

    load_task = PythonOperator(
        task_id="load",
        python_callable=run_load,
    )

    extract_task >> transform_task >> load_task
