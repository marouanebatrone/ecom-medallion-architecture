from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime
import json
import psycopg2
import os

DB_CONN = {
    "host": "host.docker.internal", 
    "database": "observers_db",         # Replace with your actual observers database name if different
    "user": "postgres",
    "password": "admin",
    "port": 5433
}

def get_db_connection():
    return psycopg2.connect(**DB_CONN)

def parse_and_store_runs(**context):
    results_path = '/opt/airflow/dbt_project/target/run_results.json'
    manifest_path = '/opt/airflow/dbt_project/target/manifest.json'
    
    if not os.path.exists(results_path):
        raise FileNotFoundError("dbt run_results.json not found.")

    with open(results_path) as f:
        run_results = json.load(f)
        
    with open(manifest_path) as f:
        manifest = json.load(f)

    conn = get_db_connection()
    cursor = conn.cursor()
    
    failed_runs = [] # Keep track of failures to pass to Task 3
    
    for result in run_results['results']:
        unique_id = result['unique_id']
        status = result['status'] # pass, fail, error
        
        # Extract observer_id from dbt's manifest via the test's meta config
        node = manifest['nodes'].get(unique_id)
        if not node or 'observer_id' not in node.get('config', {}).get('meta', {}):
            continue # Skip if it's not mapped to an observer
            
        observer_id = node['config']['meta']['observer_id']
        metadata = json.dumps({"dbt_unique_id": unique_id, "dbt_message": result.get('message')})
        
        # Insert into observer_runs and return the generated run_id
        cursor.execute("""
            INSERT INTO observer_runs (observer_id, status, metadata)
            VALUES (%s, %s, %s)
            RETURNING run_id;
        """, (observer_id, status, metadata))
        
        run_id = cursor.fetchone()[0]
        
        if status in ['fail', 'error']:
            failed_runs.append({"observer_id": observer_id, "run_id": run_id})

    conn.commit()
    cursor.close()
    conn.close()
    
    # Push failed runs to XCom for Task 3
    context['ti'].xcom_push(key='failed_runs', value=failed_runs)


def create_incidents(**context):
    failed_runs = context['ti'].xcom_pull(task_ids='parse_and_store_runs', key='failed_runs')
    
    if not failed_runs:
        print("No failed runs detected. No incidents to create.")
        return

    conn = get_db_connection()
    cursor = conn.cursor()
    
    for failure in failed_runs:
        cursor.execute("""
            INSERT INTO incidents (observer_id, run_id, severity)
            VALUES (%s, %s, 'high');
        """, (failure['observer_id'], failure['run_id']))
        
    conn.commit()
    cursor.close()
    conn.close()


with DAG('daily_data_observers', start_date=datetime(2026, 3, 15), schedule='0 1 * * *', catchup=False) as dag:

    # Task 1: Execute dbt tests
    run_dbt_tests = BashOperator(
        task_id='run_dbt_tests',
        bash_command='cd /opt/airflow/dbt_project && dbt test'
    )

    # Task 2: Parse results and insert to observer_runs
    store_runs = PythonOperator(
        task_id='parse_and_store_runs',
        python_callable=parse_and_store_runs,
        trigger_rule='all_done',
    )

    # Task 3: Read failed runs from XCom and insert to incidents
    trigger_incidents = PythonOperator(
        task_id='create_incidents',
        python_callable=create_incidents,
    )

    run_dbt_tests >> store_runs >> trigger_incidents