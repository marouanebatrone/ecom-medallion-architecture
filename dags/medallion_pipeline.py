from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, col
from pyspark.sql.types import FloatType, IntegerType, TimestampType
import shutil


class MedallionPipeline:
    
    def __init__(self):
        self.spark = SparkSession.builder \
            .appName("MedallionPipeline") \
            .config("spark.jars.packages", "org.postgresql:postgresql:42.7.3") \
            .getOrCreate()

        self.spark.sparkContext.setLogLevel("ERROR")

        self.jdbc_url = "jdbc:postgresql://127.0.0.1:5433/"
        self.properties = {
            "user": "postgres",
            "password": "admin",
            "driver": "org.postgresql.Driver"
        }

        self.input_path    = "data/input/unprocessed/"
        self.processed_path = "data/input/processed/"

        self.files = {
            "customers":   "olist_customers_dataset.csv",
            "order_items": "olist_order_items_dataset.csv",
            "orders":      "olist_orders_dataset.csv",
            "products":    "olist_products_dataset.csv",
            "sellers":     "olist_sellers_dataset.csv",
        }

    # CSV → OLTP
    
    def ingest_to_oltp(self):

        # CUSTOMERS
        df = self.spark.read.option("header", True).csv(self.input_path + self.files["customers"]).withColumn("date_ingested", current_timestamp())
        df.write.jdbc(self.jdbc_url + "sales_oltp", "customers", "append", self.properties)
        shutil.move(self.input_path + self.files["customers"], self.processed_path + self.files["customers"])
        print("customers ingested to OLTP")

        # ORDER ITEMS
        df = self.spark.read.option("header", True).csv(self.input_path + self.files["order_items"])
        df = df.withColumn("order_item_id", col("order_item_id").cast(IntegerType())) \
               .withColumn("shipping_limit_date", col("shipping_limit_date").cast(TimestampType())) \
               .withColumn("price", col("price").cast(FloatType())) \
               .withColumn("freight_value", col("freight_value").cast(FloatType())) \
               .withColumn("date_ingested", current_timestamp())
        df.write.jdbc(self.jdbc_url + "sales_oltp", "order_items", "append", self.properties)
        shutil.move(self.input_path + self.files["order_items"], self.processed_path + self.files["order_items"])
        print("order_items ingested to OLTP")

        # ORDERS
        df = self.spark.read.option("header", True).csv(self.input_path + self.files["orders"])
        for c in ["order_purchase_timestamp", "order_approved_at", 
                  "order_delivered_carrier_date", 
                  "order_delivered_customer_date", 
                  "order_estimated_delivery_date"]:
            df = df.withColumn(c, col(c).cast(TimestampType()))
        df = df.withColumn("date_ingested", current_timestamp())
        df.write.jdbc(self.jdbc_url + "sales_oltp", "orders", "append", self.properties)
        shutil.move(self.input_path + self.files["orders"], self.processed_path + self.files["orders"])
        print("orders ingested to OLTP")

        # PRODUCTS
        df = self.spark.read.option("header", True).csv(self.input_path + self.files["products"])
        df = df.withColumn("product_name_lenght", col("product_name_lenght").cast(IntegerType())) \
               .withColumn("product_description_lenght", col("product_description_lenght").cast(IntegerType())) \
               .withColumn("product_photos_qty", col("product_photos_qty").cast(IntegerType())) \
               .withColumn("product_weight_g", col("product_weight_g").cast(FloatType())) \
               .withColumn("product_length_cm", col("product_length_cm").cast(FloatType())) \
               .withColumn("product_height_cm", col("product_height_cm").cast(FloatType())) \
               .withColumn("product_width_cm", col("product_width_cm").cast(FloatType())) \
               .withColumn("date_ingested", current_timestamp())
        df.write.jdbc(self.jdbc_url + "sales_oltp", "products", "append", self.properties)
        shutil.move(self.input_path + self.files["products"], self.processed_path + self.files["products"])
        print("products ingested to OLTP")

        # SELLERS
        df = self.spark.read.option("header", True).csv(self.input_path + self.files["sellers"]).withColumn("date_ingested", current_timestamp())
        df.write.jdbc(self.jdbc_url + "sales_oltp", "sellers", "append", self.properties)
        shutil.move(self.input_path + self.files["sellers"], self.processed_path + self.files["sellers"])
        print("sellers ingested to OLTP")

    # OLTP → BRONZE

    def oltp_to_bronze(self):
        for table in self.files.keys():
            df = self.spark.read.jdbc(
                url=self.jdbc_url + "sales_oltp", 
                table=f"(SELECT * FROM {table} WHERE date_ingested::date = CURRENT_DATE) as t",
                properties=self.properties
            )
            df.write.jdbc(self.jdbc_url + "sales_bronze", table, "append", self.properties)
            print(f"{table} loaded to Bronze")

    # BRONZE → SILVER

    def bronze_to_silver(self):

        # CUSTOMERS
        customers = self.spark.read.jdbc(
            self.jdbc_url + "sales_bronze",
            "(SELECT * FROM customers WHERE date_ingested::date = CURRENT_DATE) as t",
            properties=self.properties
        )
        customers.select(
            col("customer_id"),
            col("customer_zip_code_prefix").alias("customer_zip_code"),
            col("customer_city"),
            col("customer_state"),
            col("date_ingested")
        ).dropDuplicates(["customer_id"]) \
         .write.jdbc(self.jdbc_url + "sales_silver", "customers", "append", self.properties)
        print("customers → Silver")

        # ORDERS
        orders = self.spark.read.jdbc(
            self.jdbc_url + "sales_bronze",
            "(SELECT * FROM orders WHERE date_ingested::date = CURRENT_DATE) as t",
            properties=self.properties
        )
        orders.select(
            col("order_id"),
            col("customer_id"),
            col("order_purchase_timestamp").cast(TimestampType()).alias("purchase_timestamp"),
            col("date_ingested")
        ).write.jdbc(self.jdbc_url + "sales_silver", "orders", "append", self.properties)
        print("orders → Silver")

        # ORDER ITEMS
        order_items = self.spark.read.jdbc(
            self.jdbc_url + "sales_bronze",
            "(SELECT * FROM order_items WHERE date_ingested::date = CURRENT_DATE) as t",
            properties=self.properties
        )
        order_items.select(
            col("order_id"),
            col("product_id"),
            col("seller_id"),
            col("price").cast(FloatType()),
            col("freight_value").cast(FloatType()),
            col("date_ingested")
        ).write.jdbc(self.jdbc_url + "sales_silver", "order_items", "append", self.properties)
        print("order_items → Silver")

        # PRODUCTS
        products = self.spark.read.jdbc(
            self.jdbc_url + "sales_bronze",
            "(SELECT * FROM products WHERE date_ingested::date = CURRENT_DATE) as t",
            properties=self.properties
        )
        products.select(
            col("product_id"),
            col("product_category_name"),
            col("product_weight_g").cast(FloatType()),
            col("product_length_cm").cast(FloatType()),
            col("product_height_cm").cast(FloatType()),
            col("product_width_cm").cast(FloatType()),
            col("date_ingested")
        ).dropDuplicates(["product_id"]) \
         .write.jdbc(self.jdbc_url + "sales_silver", "products", "append", self.properties)
        print("products → Silver")

        # SELLERS
        sellers = self.spark.read.jdbc(
            self.jdbc_url + "sales_bronze",
            "(SELECT * FROM sellers WHERE date_ingested::date = CURRENT_DATE) as t",
            properties=self.properties
        )
        sellers.select(
            col("seller_id"),
            col("seller_zip_code_prefix").cast(IntegerType()).alias("seller_zip_code"),
            col("seller_city"),
            col("seller_state"),
            col("date_ingested")
        ).dropDuplicates(["seller_id"]) \
         .write.jdbc(self.jdbc_url + "sales_silver", "sellers", "append", self.properties)
        print("sellers → Silver")

        print("Bronze → Silver completed")

    # SILVER → GOLD

    def silver_to_gold(self):

        orders = self.spark.read.jdbc(
            self.jdbc_url + "sales_silver",
            "(SELECT * FROM orders WHERE date_ingested::date = CURRENT_DATE) as t",
            properties=self.properties
        )
        order_items = self.spark.read.jdbc(
            self.jdbc_url + "sales_silver",
            "(SELECT * FROM order_items WHERE date_ingested::date = CURRENT_DATE) as t",
            properties=self.properties
        )
        customers = self.spark.read.jdbc(
            self.jdbc_url + "sales_silver",
            "(SELECT * FROM customers WHERE date_ingested::date = CURRENT_DATE) as t",
            properties=self.properties
        )
        products = self.spark.read.jdbc(
            self.jdbc_url + "sales_silver",
            "(SELECT * FROM products WHERE date_ingested::date = CURRENT_DATE) as t",
            properties=self.properties
        )
        sellers = self.spark.read.jdbc(
            self.jdbc_url + "sales_silver",
            "(SELECT * FROM sellers WHERE date_ingested::date = CURRENT_DATE) as t",
            properties=self.properties
        )

        # FACT TABLE
        fact_sales = orders.join(order_items, "order_id").select(
            col("order_id"),
            col("customer_id"),
            col("seller_id"),
            col("product_id"),
            col("purchase_timestamp"),
            col("price"),
            col("freight_value"),
            orders["date_ingested"]
        )
        fact_sales.write.jdbc(self.jdbc_url + "sales_gold", "fact_sales", "append", self.properties)
        print("fact_sales → Gold")

        customers.write.jdbc(self.jdbc_url + "sales_gold", "dim_customers", "append", self.properties)
        print("dim_customers → Gold")

        products.write.jdbc(self.jdbc_url + "sales_gold", "dim_products", "append", self.properties)
        print("dim_products → Gold")

        sellers.write.jdbc(self.jdbc_url + "sales_gold", "dim_sellers", "append", self.properties)
        print("dim_sellers → Gold")

        print("Silver → Gold completed")