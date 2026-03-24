from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'qupid',
}

with DAG(
    dag_id="data_observers",
    default_args=default_args,
    description='Executes dynamic data quality observers and logs incidents',
    schedule='0 1 * * *',
    start_date=datetime(2026, 3, 15),
    catchup=False,
    tags=['observability', 'data_quality'],
) as dag:
    run_observers = BashOperator(
        task_id='run_observer_pipeline',
        bash_command='python /opt/airflow/scripts/observer_pipeline.py',
    )

    run_observers