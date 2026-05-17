import sys
import os
from medallion_pipeline import MedallionPipeline

INPUT_PATH = "/opt/airflow/data/input/unprocessed/"
FILES = [
    "olist_customers_dataset.csv",
    "olist_order_items_dataset.csv",
    "olist_orders_dataset.csv",
    "olist_products_dataset.csv",
    "olist_sellers_dataset.csv",
]
SKIP_FILE = "/tmp/airflow_skip_pipeline"


def ingest():
    if not any(os.path.exists(INPUT_PATH + f) for f in FILES):
        print("No files to process. Skipping pipeline.")
        open(SKIP_FILE, "w").close()
        sys.exit(0)
    if os.path.exists(SKIP_FILE):
        os.remove(SKIP_FILE)
    with MedallionPipeline() as pipeline:
        pipeline.ingest_to_oltp()


def bronze():
    if os.path.exists(SKIP_FILE):
        print("No files were ingested. Skipping.")
        sys.exit(0)
    with MedallionPipeline() as pipeline:
        pipeline.oltp_to_bronze()


def silver():
    if os.path.exists(SKIP_FILE):
        print("No files were ingested. Skipping.")
        sys.exit(0)
    with MedallionPipeline() as pipeline:
        pipeline.bronze_to_silver()


def gold():
    if os.path.exists(SKIP_FILE):
        print("No files were ingested. Skipping.")
        sys.exit(0)
    with MedallionPipeline() as pipeline:
        pipeline.silver_to_gold()


if __name__ == "__main__":
    commands = {
        "ingest": ingest,
        "bronze": bronze,
        "silver": silver,
        "gold":   gold,
    }
    if len(sys.argv) < 2 or sys.argv[1] not in commands:
        print(f"Usage: python run_pipeline.py [{' | '.join(commands)}]")
        sys.exit(1)
    commands[sys.argv[1]]()