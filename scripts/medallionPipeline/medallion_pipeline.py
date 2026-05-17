from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, col
from pyspark.sql.types import FloatType, IntegerType, TimestampType
import shutil
import time
import os
from dotenv import load_dotenv

load_dotenv("/opt/airflow/.env")


class MedallionPipeline:

    def __init__(self):
        openlineage_package = "io.openlineage:openlineage-spark_2.12:1.43.0"
        postgres_package = "org.postgresql:postgresql:42.7.3"

        OPENLINEAGE_URL = os.getenv("OPENLINEAGE_URL")
        OPENLINEAGE_ENDPOINT = os.getenv("OPENLINEAGE_ENDPOINT")
        OPENLINEAGE_NAMESPACE = os.getenv("OPENLINEAGE_NAMESPACE")

        JDB_URL = os.getenv("JDB_URL")
        JDB_USER = os.getenv("JDB_USER")
        JDB_PASSWORD = os.getenv("JDB_PASSWORD")
        JDB_DRIVER = os.getenv("JDB_DRIVER")

        INPUT_PATH = os.getenv("INPUT_PATH")
        PROCESSED_PATH = os.getenv("PROCESSED_PATH")

        self.spark = SparkSession.builder \
            .appName("MedallionPipeline") \
            .config("spark.jars.packages", f"{postgres_package},{openlineage_package}") \
            .config("spark.extraListeners", "io.openlineage.spark.agent.OpenLineageSparkListener") \
            .config("spark.openlineage.transport.type", "http") \
            .config("spark.openlineage.transport.url", OPENLINEAGE_URL) \
            .config("spark.openlineage.transport.endpoint", OPENLINEAGE_ENDPOINT) \
            .config("spark.openlineage.namespace", OPENLINEAGE_NAMESPACE) \
            .config("spark.openlineage.facets.spark.logicalPlan.disabled", "false") \
            .config("spark.openlineage.facets.debug.disabled", "false") \
            .config("spark.openlineage.facets.spark_unknown.disabled", "false") \
            .config("spark.openlineage.columnLineage.datasetLineageEnabled", "true") \
            .config("spark.openlineage.capturedProperties",
                    "spark.master,spark.app.name,spark.sql.shuffle.partitions,"
                    "spark.executor.memory,spark.executor.cores,spark.driver.memory") \
            .config("spark.openlineage.job.tags", "env:production;team:data-engineering;pipeline:medallion") \
            .config("spark.openlineage.run.tags", "pipeline:daily-etl") \
            .config("spark.openlineage.job.owners.team", "rca-team") \
            .config("spark.openlineage.jobName.replaceDotWithUnderscore", "true") \
            .getOrCreate()

        self.spark.sparkContext.setLogLevel("ERROR")

        self.jdbc_url = JDB_URL

        self.properties = {
            "user": JDB_USER,
            "password": JDB_PASSWORD,
            "driver": JDB_DRIVER,
        }

        self.input_path = INPUT_PATH
        self.processed_path = PROCESSED_PATH

        self.files = {
            "customers":   "olist_customers_dataset.csv",
            "order_items": "olist_order_items_dataset.csv",
            "orders":      "olist_orders_dataset.csv",
            "products":    "olist_products_dataset.csv",
            "sellers":     "olist_sellers_dataset.csv",
        }

    # ─── Context manager: guarantees lineage flush on exit ───────────────
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Flush OpenLineage events and stop the SparkSession.

        Runs whether the stage succeeded or raised. On failure, this gives
        the OpenLineage listener time to send the FAIL event for the broken
        Spark job before the JVM exits.
        """
        if exc_type is not None:
            print(f"Stage failed with {exc_type.__name__}; flushing lineage events...")
        else:
            print("Stage succeeded; flushing lineage events...")
        time.sleep(5)
        self.spark.stop()
        return False 

    # ─── Helper ──────────────────────────────────────────────────────────
    def _read_today(self, schema_table: str):
        """Read rows ingested today using JDBC with a pushdown predicate.
        """
        return self.spark.read.jdbc(
            url=self.jdbc_url,
            table=schema_table,
            predicates=["date_ingested::date = CURRENT_DATE"],
            properties=self.properties,
        )

    # ─── Stage 1: CSV → OLTP ─────────────────────────────────────────────
    def ingest_to_oltp(self):
        df = self.spark.read.option("header", True) \
            .csv(self.input_path + self.files["customers"]) \
            .withColumn("date_ingested", current_timestamp())
        df.write.jdbc(self.jdbc_url, "oltp.customers", "append", self.properties)
        shutil.move(self.input_path + self.files["customers"],
                    self.processed_path + self.files["customers"])
        print("customers ingested to OLTP")

        df = self.spark.read.option("header", True).csv(self.input_path + self.files["order_items"])
        df = df.withColumn("order_item_id", col("order_item_id").cast(IntegerType())) \
               .withColumn("shipping_limit_date", col("shipping_limit_date").cast(TimestampType())) \
               .withColumn("price", col("price").cast(FloatType())) \
               .withColumn("freight_value", col("freight_value").cast(FloatType())) \
               .withColumn("date_ingested", current_timestamp())
        df.write.jdbc(self.jdbc_url, "oltp.order_items", "append", self.properties)
        shutil.move(self.input_path + self.files["order_items"],
                    self.processed_path + self.files["order_items"])
        print("order_items ingested to OLTP")

        df = self.spark.read.option("header", True).csv(self.input_path + self.files["orders"])
        for c in ["order_purchase_timestamp", "order_approved_at",
                  "order_delivered_carrier_date",
                  "order_delivered_customer_date",
                  "order_estimated_delivery_date"]:
            df = df.withColumn(c, col(c).cast(TimestampType()))
        df = df.withColumn("date_ingested", current_timestamp())
        df.write.jdbc(self.jdbc_url, "oltp.orders", "append", self.properties)
        shutil.move(self.input_path + self.files["orders"],
                    self.processed_path + self.files["orders"])
        print("orders ingested to OLTP")

        df = self.spark.read.option("header", True).csv(self.input_path + self.files["products"])
        df = df.withColumn("product_name_lenght", col("product_name_lenght").cast(IntegerType())) \
               .withColumn("product_description_lenght", col("product_description_lenght").cast(IntegerType())) \
               .withColumn("product_photos_qty", col("product_photos_qty").cast(IntegerType())) \
               .withColumn("product_weight_g", col("product_weight_g").cast(FloatType())) \
               .withColumn("product_length_cm", col("product_length_cm").cast(FloatType())) \
               .withColumn("product_height_cm", col("product_height_cm").cast(FloatType())) \
               .withColumn("product_width_cm", col("product_width_cm").cast(FloatType())) \
               .withColumn("date_ingested", current_timestamp())
        df.write.jdbc(self.jdbc_url, "oltp.products", "append", self.properties)
        shutil.move(self.input_path + self.files["products"],
                    self.processed_path + self.files["products"])
        print("products ingested to OLTP")

        df = self.spark.read.option("header", True) \
            .csv(self.input_path + self.files["sellers"]) \
            .withColumn("date_ingested", current_timestamp())
        df.write.jdbc(self.jdbc_url, "oltp.sellers", "append", self.properties)
        shutil.move(self.input_path + self.files["sellers"],
                    self.processed_path + self.files["sellers"])
        print("sellers ingested to OLTP")

    # ─── Stage 2: OLTP → Bronze ──────────────────────────────────────────
    def oltp_to_bronze(self):
        for table in self.files.keys():
            df = self._read_today(f"oltp.{table}")
            df.write.jdbc(self.jdbc_url, f"bronze.{table}", "append", self.properties)
            print(f"{table} loaded to Bronze")

    # ─── Stage 3: Bronze → Silver ────────────────────────────────────────
    def bronze_to_silver(self):
        customers = self._read_today("bronze.customers")
        customers.select(
            col("customer_id"),
            col("customer_zip_code_prefix").alias("customer_zip_code"),
            col("customer_city"),
            col("customer_state"),
            col("date_ingested"),
        ).dropDuplicates(["customer_id"]) \
         .write.jdbc(self.jdbc_url, "silver.customers", "append", self.properties)
        print("customers → Silver")

        orders = self._read_today("bronze.orders")
        orders.select(
            col("order_id"),
            col("customer_id"),
            col("order_purchase_timestamp").cast(TimestampType()).alias("purchase_timestamp"),
            col("date_ingested"),
        ).write.jdbc(self.jdbc_url, "silver.orders", "append", self.properties)
        print("orders → Silver")

        order_items = self._read_today("bronze.order_items")
        order_items.select(
            col("order_id"),
            col("product_id"),
            col("seller_id"),
            col("price").cast(FloatType()),
            col("freight_value").cast(FloatType()),
            col("date_ingested"),
        ).write.jdbc(self.jdbc_url, "silver.order_items", "append", self.properties)
        print("order_items → Silver")

        products = self._read_today("bronze.products")
        products.select(
            col("product_id"),
            col("product_category_name"),
            col("product_weight_g").cast(FloatType()),
            col("product_length_cm").cast(FloatType()),
            col("product_height_cm").cast(FloatType()),
            col("product_width_cm").cast(FloatType()),
            col("date_ingested"),
        ).dropDuplicates(["product_id"]) \
         .write.jdbc(self.jdbc_url, "silver.products", "append", self.properties)
        print("products → Silver")

        sellers = self._read_today("bronze.sellers")
        sellers.select(
            col("seller_id"),
            col("seller_zip_code_prefix").cast(IntegerType()).alias("seller_zip_code"),
            col("seller_city"),
            col("seller_state"),
            col("date_ingested"),
        ).dropDuplicates(["seller_id"]) \
         .write.jdbc(self.jdbc_url, "silver.sellers", "append", self.properties)
        print("sellers → Silver")

        print("Bronze → Silver completed")

    # ─── Stage 4: Silver → Gold ──────────────────────────────────────────
    def silver_to_gold(self):
        orders      = self._read_today("silver.orders")
        order_items = self._read_today("silver.order_items")
        customers   = self._read_today("silver.customers")
        products    = self._read_today("silver.products")
        sellers     = self._read_today("silver.sellers")

        fact_sales = orders.join(order_items, "order_id").select(
            col("order_id"),
            col("customer_id"),
            col("seller_id"),
            col("product_id"),
            col("purchase_timestamp"),
            col("price"),
            col("freight_value"),
            orders["date_ingested"],
        )
        fact_sales.write.jdbc(self.jdbc_url, "gold.fact_sales", "append", self.properties)
        print("fact_sales → Gold")

        customers.write.jdbc(self.jdbc_url, "gold.dim_customers", "append", self.properties)
        print("dim_customers → Gold")

        products.write.jdbc(self.jdbc_url, "gold.dim_products", "append", self.properties)
        print("dim_products → Gold")

        sellers.write.jdbc(self.jdbc_url, "gold.dim_sellers", "append", self.properties)
        print("dim_sellers → Gold")

        print("Silver → Gold completed")