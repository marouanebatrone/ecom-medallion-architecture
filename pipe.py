import shutil
import pg8000
import pandas as pd
from sqlalchemy import create_engine, text


class MedallionPipeline:

    def __init__(self):

        self.input_path     = "data/input/unprocessed/"
        self.processed_path = "data/input/processed/"

        self.files = {
            "customers":   "olist_customers_dataset.csv",
            "order_items": "olist_order_items_dataset.csv",
            "orders":      "olist_orders_dataset.csv",
            "products":    "olist_products_dataset.csv",
            "sellers":     "olist_sellers_dataset.csv",
        }

        self.engines = {
            db: create_engine(
                f"postgresql+pg8000://postgres:admin@127.0.0.1:5433/{db}",
                isolation_level="AUTOCOMMIT"
            )
            for db in ["sales_oltp", "sales_bronze", "sales_silver", "sales_gold"]
        }

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _read(self, db: str, table: str, today_only: bool = True) -> pd.DataFrame:
        query = (
            f"SELECT * FROM {table} WHERE date_ingested::date = CURRENT_DATE"
            if today_only
            else f"SELECT * FROM {table}"
        )
        with self.engines[db].connect() as conn:
            return pd.read_sql(text(query), conn)

    def _write(self, df: pd.DataFrame, db: str, table: str):
        with self.engines[db].connect() as conn:
            df.to_sql(
                table,
                conn,
                if_exists="append",
                index=False,
                method="multi",
                chunksize=500
            )

    # ------------------------------------------------------------------
    # Type casting helpers to match sales_oltp schema
    # ------------------------------------------------------------------

    def _prepare_customers(self, df: pd.DataFrame) -> pd.DataFrame:
        return df  # all text

    def _prepare_order_items(self, df: pd.DataFrame) -> pd.DataFrame:
        df["order_item_id"]       = df["order_item_id"].astype(int)
        df["shipping_limit_date"] = pd.to_datetime(df["shipping_limit_date"])
        df["price"]               = df["price"].astype(float)
        df["freight_value"]       = df["freight_value"].astype(float)
        return df

    def _prepare_orders(self, df: pd.DataFrame) -> pd.DataFrame:
        for c in ["order_purchase_timestamp", "order_approved_at",
                  "order_delivered_carrier_date", "order_delivered_customer_date",
                  "order_estimated_delivery_date"]:
            df[c] = pd.to_datetime(df[c], errors="coerce")
        return df

    def _prepare_products(self, df: pd.DataFrame) -> pd.DataFrame:
        for c in ["product_name_lenght", "product_description_lenght", "product_photos_qty"]:
            df[c] = pd.to_numeric(df[c], errors="coerce").astype("Int64")
        for c in ["product_weight_g", "product_length_cm", "product_height_cm", "product_width_cm"]:
            df[c] = pd.to_numeric(df[c], errors="coerce").astype(float)
        return df

    def _prepare_sellers(self, df: pd.DataFrame) -> pd.DataFrame:
        return df  # all text

    # ------------------------------------------------
    # 1️⃣ CSV → OLTP
    # ------------------------------------------------

    def ingest_to_oltp(self):
        preparers = {
            "customers":   self._prepare_customers,
            "order_items": self._prepare_order_items,
            "orders":      self._prepare_orders,
            "products":    self._prepare_products,
            "sellers":     self._prepare_sellers,
        }

        for table, file in self.files.items():
            path = self.input_path + file
            df = pd.read_csv(path)
            df = preparers[table](df)
            df["date_ingested"] = pd.Timestamp.now()
            self._write(df, "sales_oltp", table)
            shutil.move(path, self.processed_path + file)
            print(f"✅ {file} ingested into OLTP")

    # ------------------------------------------------
    # 2️⃣ OLTP → BRONZE (incremental)
    # ------------------------------------------------

    def oltp_to_bronze(self):
        for table in self.files.keys():
            df = self._read("sales_oltp", table, today_only=True)
            self._write(df, "sales_bronze", table)
            print(f"✅ {table} moved to Bronze")

    # ------------------------------------------------
    # 3️⃣ BRONZE → SILVER (clean + cast types)
    # ------------------------------------------------

    def bronze_to_silver(self):

        # CUSTOMERS
        customers = self._read("sales_bronze", "customers")
        customers = customers[["customer_id", "customer_zip_code_prefix",
                                "customer_city", "customer_state", "date_ingested"]] \
            .rename(columns={"customer_zip_code_prefix": "customer_zip_code"}) \
            .drop_duplicates(subset=["customer_id"])
        self._write(customers, "sales_silver", "customers")
        print("✅ customers → Silver")

        # ORDERS
        orders = self._read("sales_bronze", "orders")
        orders = orders[["order_id", "customer_id",
                          "order_purchase_timestamp", "date_ingested"]] \
            .rename(columns={"order_purchase_timestamp": "purchase_timestamp"})
        orders["purchase_timestamp"] = pd.to_datetime(orders["purchase_timestamp"])
        self._write(orders, "sales_silver", "orders")
        print("✅ orders → Silver")

        # ORDER ITEMS
        order_items = self._read("sales_bronze", "order_items")
        order_items = order_items[["order_id", "product_id", "seller_id",
                                    "price", "freight_value", "date_ingested"]]
        order_items["price"]         = order_items["price"].astype(float)
        order_items["freight_value"] = order_items["freight_value"].astype(float)
        self._write(order_items, "sales_silver", "order_items")
        print("✅ order_items → Silver")

        # PRODUCTS
        products = self._read("sales_bronze", "products")
        products = products[["product_id", "product_category_name",
                              "product_weight_g", "product_length_cm",
                              "product_height_cm", "product_width_cm", "date_ingested"]] \
            .drop_duplicates(subset=["product_id"])
        for c in ["product_weight_g", "product_length_cm",
                  "product_height_cm", "product_width_cm"]:
            products[c] = products[c].astype(float)
        self._write(products, "sales_silver", "products")
        print("✅ products → Silver")

        # SELLERS
        sellers = self._read("sales_bronze", "sellers")
        sellers = sellers[["seller_id", "seller_zip_code_prefix",
                            "seller_city", "seller_state", "date_ingested"]] \
            .rename(columns={"seller_zip_code_prefix": "seller_zip_code"}) \
            .drop_duplicates(subset=["seller_id"])
        sellers["seller_zip_code"] = sellers["seller_zip_code"].astype("Int64")
        self._write(sellers, "sales_silver", "sellers")
        print("✅ sellers → Silver")

        print("✅ Bronze → Silver completed")

    # ------------------------------------------------
    # 4️⃣ SILVER → GOLD (fact + dimensions)
    # ------------------------------------------------

    def silver_to_gold(self):

        orders      = self._read("sales_silver", "orders")
        order_items = self._read("sales_silver", "order_items")
        customers   = self._read("sales_silver", "customers")
        products    = self._read("sales_silver", "products")
        sellers     = self._read("sales_silver", "sellers")

        # FACT TABLE
        fact_sales = orders.merge(order_items, on="order_id", how="inner")
        fact_sales = fact_sales[["order_id", "customer_id", "seller_id",
                                  "product_id", "purchase_timestamp",
                                  "price", "freight_value", "date_ingested_x"]] \
            .rename(columns={"date_ingested_x": "date_ingested"})
        self._write(fact_sales, "sales_gold", "fact_sales")
        print("✅ fact_sales → Gold")

        self._write(customers, "sales_gold", "dim_customers")
        print("✅ dim_customers → Gold")

        self._write(products, "sales_gold", "dim_products")
        print("✅ dim_products → Gold")

        self._write(sellers, "sales_gold", "dim_sellers")
        print("✅ dim_sellers → Gold")

        print("✅ Silver → Gold completed")