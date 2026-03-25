import sys
import os
from medallion_pipeline import MedallionPipeline

INPUT_PATH = "/opt/airflow/data/input/unprocessed/"
FILES = [
    "olist_customers_dataset.csv",
    "olist_order_items_dataset.csv",
    "olist_orders_dataset.csv",
    "olist_products_dataset.csv",
    "olist_sellers_dataset.csv"
]

def get_pipeline():
    return MedallionPipeline()

def ingest():
    if not any(os.path.exists(INPUT_PATH + f) for f in FILES):
        print("No files to process. Skipping pipeline.")
        open("/tmp/airflow_skip_pipeline", "w").close()
        sys.exit(0)
    if os.path.exists("/tmp/airflow_skip_pipeline"):
        os.remove("/tmp/airflow_skip_pipeline")
    get_pipeline().ingest_to_oltp()

def bronze():
    if os.path.exists("/tmp/airflow_skip_pipeline"):
        print("No files were ingested. Skipping.")
        sys.exit(0)
    get_pipeline().oltp_to_bronze()

def silver():
    if os.path.exists("/tmp/airflow_skip_pipeline"):
        print("No files were ingested. Skipping.")
        sys.exit(0)
    get_pipeline().bronze_to_silver()

def gold():
    if os.path.exists("/tmp/airflow_skip_pipeline"):
        print("No files were ingested. Skipping.")
        sys.exit(0)
    get_pipeline().silver_to_gold()


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