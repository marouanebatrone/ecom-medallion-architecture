CREATE TABLE customers(
customer_id TEXT,
customer_zip_code TEXT,
customer_city TEXT,
customer_state TEXT,
date_ingested TIMESTAMP
);

CREATE TABLE order_items(
order_id TEXT,
product_id TEXT,
seller_id TEXT,
price FLOAT,
freight_value FLOAT,
date_ingested TIMESTAMP
);

CREATE TABLE orders(
order_id TEXT,
customer_id TEXT,
purchase_timestamp TIMESTAMP,
date_ingested TIMESTAMP
);

CREATE TABLE products(
product_id TEXT,
product_category_name TEXT,
product_weight_g FLOAT,
product_length_cm FLOAT,
product_height_cm FLOAT,
product_width_cm FLOAT,
date_ingested TIMESTAMP
);

CREATE TABLE sellers(
seller_id TEXT,
seller_zip_code INT,
seller_city TEXT,
seller_state TEXT,
date_ingested TIMESTAMP
);