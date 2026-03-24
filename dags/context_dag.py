from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys
import os

# Ensure Airflow can find your script if it's in a different folder
sys.path.insert(0, '/opt/airflow/scripts')

from context_builder import IncidentContextBuilder

default_args = {
    'owner': 'qupid',
}

def execute_context_builder():
    # Use Airflow Connections in production, but hardcoded here for testing
    OBS_DB = {
        "dbname": "observers_db", "user": "postgres", 
        "password": "postgres", "host": "host.docker.internal", "port": 5433
    }
    TARGET_DB = {
        "dbname": "sales_data_platform", "user": "postgres", 
        "password": "postgres", "host": "host.docker.internal", "port": 5433
    }
    
    builder = IncidentContextBuilder(OBS_DB, TARGET_DB)
    # If running in Airflow Docker, override output dir to a shared volume
    builder.output_dir = "/opt/airflow/incidents" 
    os.makedirs(builder.output_dir, exist_ok=True)
    
    builder.run()

with DAG(
    'daily_context_builder',
    default_args=default_args,
    description='Builds JSON context payloads for the Qupid RCA AI Agent',
    schedule_interval='0 2 * * *', 
    start_date=datetime(2026, 3, 18),
    catchup=False,
    tags=['rca', 'observability', 'llm'],
) as dag:

    build_context_task = PythonOperator(
        task_id='generate_incident_json_payloads',
        python_callable=execute_context_builder,
    )

    build_context_task