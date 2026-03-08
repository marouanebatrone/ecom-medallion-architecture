from medallion_pipeline import MedallionPipeline
import os

pipeline = MedallionPipeline()

input_path = "data/input/unprocessed/"
files = [
    "olist_customers_dataset.csv",
    "olist_order_items_dataset.csv",
    "olist_orders_dataset.csv",
    "olist_products_dataset.csv",
    "olist_sellers_dataset.csv"
]

if not any(os.path.exists(input_path + f) for f in files):
    print("No files to process in unprocessed folder. Exiting.")
else:
    pipeline.ingest_to_oltp()
    pipeline.oltp_to_bronze()
    pipeline.bronze_to_silver()
    pipeline.silver_to_gold()