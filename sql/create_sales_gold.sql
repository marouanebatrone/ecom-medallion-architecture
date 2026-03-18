CREATE TABLE fact_sales(
order_id TEXT,
customer_id TEXT,
seller_id TEXT,
product_id TEXT,
purchase_timestamp TIMESTAMP,
price FLOAT,
freight_value FLOAT,
date_ingested TIMESTAMP
);

CREATE TABLE dim_customers(
customer_id TEXT,
customer_zip_code TEXT,
customer_city TEXT,
customer_state TEXT,
date_ingested TIMESTAMP
);

CREATE TABLE dim_products(
product_id TEXT,
product_category_name TEXT,
product_weight_g FLOAT,
product_length_cm FLOAT,
product_height_cm FLOAT,
product_width_cm FLOAT,
date_ingested TIMESTAMP
);

CREATE TABLE dim_sellers(
seller_id TEXT,
seller_zip_code INT,
seller_city TEXT,
seller_state TEXT,
date_ingested TIMESTAMP
);