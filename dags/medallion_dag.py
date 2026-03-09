from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import os


def run_pipeline():
    from medallion_pipeline import MedallionPipeline

    input_path = "/opt/airflow/data/input/unprocessed/"
    files = [
        "olist_customers_dataset.csv",
        "olist_order_items_dataset.csv",
        "olist_orders_dataset.csv",
        "olist_products_dataset.csv",
        "olist_sellers_dataset.csv"
    ]

    if not any(os.path.exists(input_path + f) for f in files):
        print("No files to process in unprocessed folder. Exiting.")
        return

    pipeline = MedallionPipeline()
    pipeline.ingest_to_oltp()
    pipeline.oltp_to_bronze()
    pipeline.bronze_to_silver()
    pipeline.silver_to_gold()
    print("Medallion pipeline completed successfully.")


with DAG(
    dag_id="medallion_pipeline_dag",
    description="Daily medallion pipeline: CSV → OLTP → Bronze → Silver → Gold",
    schedule="0 0 * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["medallion", "etl"]
) as dag:

    run_pipeline_task = PythonOperator(
        task_id="run_medallion_pipeline",
        python_callable=run_pipeline
    )