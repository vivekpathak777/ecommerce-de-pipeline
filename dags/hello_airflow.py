from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator


# Define the Python function Airflow will execute
def say_hello():
    print("Hello! My first Airflow DAG is running!")


# Define our workflow
with DAG(
    dag_id="hello_airflow",
    start_date=datetime(2026, 1, 1),
    schedule=None,       # Run manually, not automatically
    catchup=False,       # Don't run missed historical schedules
    tags=["learning"],
) as dag:

    # Create a task that executes our Python function
    hello_task = PythonOperator(
        task_id="say_hello",
        python_callable=say_hello,
    )