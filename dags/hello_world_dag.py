from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from datetime import datetime


def print_message():
    print("Hello from Airflow!")


with DAG(
    dag_id="hello_world_dag",
    start_date=datetime(2024, 1, 1),
    schedule="*/5 * * * *",
    catchup=False,
    tags=["tutorial"],
) as dag:

    bash_task = BashOperator(
        task_id="show_date",
        bash_command="date",
    )

    python_task = PythonOperator(
        task_id="print_message",
        python_callable=print_message,
    )

    bash_task >> python_task