from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

PIPELINE_CMD = "cd /opt/airflow && python run_pipeline.py"

with DAG(
    dag_id="medallion_pipeline_dag",
    description="Daily medallion pipeline: CSV → OLTP → Bronze → Silver → Gold",
    schedule="0 0 * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["medallion", "etl"]
) as dag:

    ingest_to_oltp = BashOperator(
        task_id="ingest_to_oltp",
        bash_command=f"{PIPELINE_CMD} ingest"
    )

    oltp_to_bronze = BashOperator(
        task_id="oltp_to_bronze",
        bash_command=f"{PIPELINE_CMD} bronze"
    )

    bronze_to_silver = BashOperator(
        task_id="bronze_to_silver",
        bash_command=f"{PIPELINE_CMD} silver"
    )

    silver_to_gold = BashOperator(
        task_id="silver_to_gold",
        bash_command=f"{PIPELINE_CMD} gold"
    )

    ingest_to_oltp >> oltp_to_bronze >> bronze_to_silver >> silver_to_gold