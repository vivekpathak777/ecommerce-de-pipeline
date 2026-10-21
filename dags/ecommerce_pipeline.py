
from datetime import datetime, timedelta
import time

import requests
from airflow import DAG
from airflow.hooks.base import BaseHook
from airflow.operators.python import PythonOperator


# IDs of the jobs we already created and tested
DATABRICKS_JOB_ID = 903601673037885
DBT_ACCOUNT_ID = 70506183168904
DBT_JOB_ID = 70506183143079

POLL_INTERVAL = 15  # Check job status every 15 seconds
TIMEOUT_SECONDS = 1800  # Stop waiting after 30 minutes


def trigger_databricks():
    # Retrieve the saved Airflow connection
    conn = BaseHook.get_connection("databricks_api")
    host = conn.host.rstrip("/")
    headers = {"Authorization": f"Bearer {conn.password}"}

    # Start our Databricks Bronze + Silver job
    response = requests.post(
        f"{host}/api/2.1/jobs/run-now",
        headers=headers,
        json={"job_id": DATABRICKS_JOB_ID},
        timeout=30,
    )
    response.raise_for_status()

    run_id = response.json()["run_id"]
    print(f"Databricks run started: {run_id}")

    # Wait until Databricks finishes both notebook tasks
    deadline = time.monotonic() + TIMEOUT_SECONDS

    while time.monotonic() < deadline:
        result = requests.get(
            f"{host}/api/2.1/jobs/runs/get",
            headers=headers,
            params={"run_id": run_id},
            timeout=30,
        )
        result.raise_for_status()

        state = result.json()["state"]
        lifecycle = state["life_cycle_state"]

        if lifecycle in ("TERMINATED", "SKIPPED", "INTERNAL_ERROR"):
            if state.get("result_state") == "SUCCESS":
                print("Databricks Bronze + Silver completed!")
                return

            raise RuntimeError(f"Databricks job failed: {state}")

        time.sleep(POLL_INTERVAL)

    raise TimeoutError(f"Databricks run {run_id} timed out")


def trigger_dbt():
    # Retrieve dbt Cloud API credentials
    conn = BaseHook.get_connection("dbt_cloud_api")
    host = conn.host.rstrip("/")

    headers = {
        "Authorization": f"Token {conn.password}",
        "Content-Type": "application/json",
    }

    # Trigger our dbt Gold transformation deployment job
    response = requests.post(
        f"{host}/api/v2/accounts/{DBT_ACCOUNT_ID}/jobs/{DBT_JOB_ID}/run/",
        headers=headers,
        json={"cause": "Triggered by Airflow"},
        timeout=30,
    )
    response.raise_for_status()

    run_id = response.json()["data"]["id"]
    print(f"dbt Cloud run started: {run_id}")

    # Poll the API until dbt finishes its models and tests
    deadline = time.monotonic() + TIMEOUT_SECONDS

    while time.monotonic() < deadline:
        result = requests.get(
            f"{host}/api/v2/accounts/{DBT_ACCOUNT_ID}/runs/{run_id}/",
            headers=headers,
            timeout=30,
        )
        result.raise_for_status()

        run = result.json()["data"]
        status = run["status"]

        if status == 10:  # Success
            print("dbt Gold transformations and tests completed!")
            return

        if status in (20, 30):  # Error or cancelled
            raise RuntimeError(f"dbt Cloud run failed: {run}")

        time.sleep(POLL_INTERVAL)

    raise TimeoutError(f"dbt Cloud run {run_id} timed out")


# Define our complete ecommerce workflow
with DAG(
    dag_id="ecommerce_end_to_end_pipeline",
    start_date=datetime(2026, 10, 1),
    schedule=None,  # Manually triggered for now
    catchup=False,
    max_active_runs=1,
    default_args={
        "retries": 1,
        "retry_delay": timedelta(minutes=2),
    },
    tags=["ecommerce", "databricks", "dbt"],
) as dag:

    # First execute Databricks Bronze and Silver
    databricks_task = PythonOperator(
        task_id="run_databricks_etl",
        python_callable=trigger_databricks,
    )

    # Then execute dbt Gold models and tests
    dbt_task = PythonOperator(
        task_id="run_dbt_gold",
        python_callable=trigger_dbt,
    )

    # dbt starts only after Databricks succeeds
    databricks_task >> dbt_task
