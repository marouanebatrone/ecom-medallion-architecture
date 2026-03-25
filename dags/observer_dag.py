from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import os

default_args = {
    'owner': 'qupid',
}

OBSERVERS_PATH = "/opt/airflow/scripts/observers"

with DAG(
    dag_id="data_observers",
    default_args=default_args,
    description='Executes dynamic data quality observers and logs incidents',
    schedule='0 1 * * *',
    start_date=datetime(2026, 3, 15),
    catchup=False,
    tags=['observability', 'data_quality', 'solid'],
) as dag:

    run_observers = BashOperator(
        task_id='run_observer_pipeline',
        bash_command=f'cd {OBSERVERS_PATH} && python3 observer_pipeline.py',
        env={
            'PYTHONPATH': f"{os.getenv('PYTHONPATH', '')}:{OBSERVERS_PATH}"
        }
    )
    
    run_observers