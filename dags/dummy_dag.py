# A minimal DAG for exploring Airflow features: operators, task
# dependencies, and XCom (passing a value from one task to another).
# Written for Airflow 3 (imports come from airflow.sdk).

from datetime import timedelta

import pendulum
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG


def say_hello(name: str = "Airflow"):
    print(f"Hello, {name}!")
    return f"Greeting sent to {name}"  # Return values are pushed to XCom.


def print_xcom_result(ti):
    result = ti.xcom_pull(task_ids="say_hello_task")
    print(f"Received from XCom: {result}")


with DAG(
    dag_id="dummy_etl_demo",
    description="A dummy DAG to explore Airflow features",
    schedule="@daily",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    default_args={
        "owner": "airflow",
        "retries": 1,
        "retry_delay": timedelta(minutes=1),
    },
    tags=["demo", "test"],
) as dag:

    start = EmptyOperator(task_id="start")

    echo_task = BashOperator(
        task_id="bash_echo",
        bash_command='echo "Running a Bash task..."',
    )

    hello_task = PythonOperator(
        task_id="say_hello_task",
        python_callable=say_hello,
        op_kwargs={"name": "Don"},
    )

    xcom_task = PythonOperator(
        task_id="xcom_check",
        python_callable=print_xcom_result,
    )

    end = EmptyOperator(task_id="end")

    start >> echo_task >> hello_task >> xcom_task >> end
