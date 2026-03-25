from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys
import os

SCRIPTS_PATH = '/opt/airflow/scripts/contextBuilder'
sys.path.insert(0, SCRIPTS_PATH)

from context_builder_orchestrator import ContextBuilderOrchestrator

default_args = {
    'owner': 'qupid',
}

def execute_context_enrichment():
    
    CONFIGS = {
        "obs_db": {
            "dbname": "observers_db", 
            "user": "postgres", 
            "password": "admin", 
            "host": "host.docker.internal", 
            "port": 5433
        },
        "target_db": {
            "dbname": "sales_data_platform", 
            "user": "postgres", 
            "password": "admin", 
            "host": "host.docker.internal", 
            "port": 5433
        },
        "mongo_uri": "mongodb://admin:admin@host.docker.internal:27017/",
        "lineage_url": "https://qupid-watchpipe.qupid.clusterdiali.me/api/v1/lineage",
        "namespace": "postgres://host.docker.internal:5433"
    }

    orchestrator = ContextBuilderOrchestrator(CONFIGS)
    
    orchestrator.run_daily_context_enrichment()

with DAG(
    'daily_context_builder',
    default_args=default_args,
    description='contet builder for Qupid RCA Agent.',
    schedule_interval='0 2 * * *',
    start_date=datetime(2026, 3, 18),
    catchup=False,
    tags=['rca', 'observability', 'mongodb'],
) as dag:

    enrich_incidents_task = PythonOperator(
        task_id='build_and_store_incident_context',
        python_callable=execute_context_enrichment,
    )

    enrich_incidents_task