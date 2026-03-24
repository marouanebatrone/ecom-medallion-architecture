from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys

sys.path.insert(0, '/opt/airflow/scripts')

from context_builder import IncidentContextBuilder

default_args = {
    'owner': 'qupid',
}

def execute_context_builder():
    OBS_DB = {
        "dbname": "observers_db",
        "user": "postgres",
        "password": "postgres",
        "host": "host.docker.internal",
        "port": 5433
    }
    
    TARGET_DB = {
        "dbname": "sales_data_platform", 
        "user": "postgres", 
        "password": "postgres", 
        "host": "host.docker.internal", 
        "port": 5433
    }

    MONGO_URI = "mongodb://admin:admin@host.docker.internal:27017/"
    
    builder = IncidentContextBuilder(OBS_DB, TARGET_DB, MONGO_URI)
    builder.run()

with DAG(
    'daily_context_builder',
    default_args=default_args,
    description='Builds incident contexts and stores them in MongoDB for the Qupid Agent',
    schedule_interval='0 2 * * *',
    start_date=datetime(2026, 3, 18),
    catchup=False,
    tags=['rca', 'observability', 'mongodb'],
) as dag:
    build_context_task = PythonOperator(
        task_id='build_and_store_incident_context',
        python_callable=execute_context_builder,
    )

    build_context_task