# E-Commerce Medallion Pipeline

A data pipeline built with **PySpark**, **PostgreSQL**, and **Apache Airflow** that processes the [Brazilian E-Commerce dataset](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce) through a medallion architecture (Bronze → Silver → Gold).

---

## Architecture
```
CSV Files
   │
   ▼
┌─────────────┐
│  OLTP Layer │  Raw ingestion with timestamp
└──────┬──────┘
       │ incremental (today only)
       ▼
┌─────────────┐
│   BRONZE    │  Exact copy of today's records
└──────┬──────┘
       │ clean + cast types
       ▼
┌─────────────┐
│   SILVER    │  Cleaned, typed, deduplicated
└──────┬──────┘
       │ join + aggregate
       ▼
┌─────────────┐
│    GOLD     │  fact_sales + dimension tables
└─────────────┘
```

---

## Project Structure
```
.
├── dags/
│   └── medallion_dag.py        # Airflow DAG definition
├── data/
│   └── input/
│       ├── unprocessed/        # Drop CSV files here
│       └── processed/          # Files moved here after ingestion
├── logs/                       # Airflow logs
├── plugins/                    # Airflow plugins
├── config/                     # Airflow config
├── medallion_pipeline.py       # PySpark pipeline logic
├── run_pipeline.py             # CLI entry point for each stage
├── Dockerfile                  # Custom Airflow image with Java + PySpark
├── docker-compose.yaml         # Airflow services
└── .env                        # Environment variables
```

---

## Database Schema

### OLTP / Bronze (raw)
| Table | Key Columns |
|---|---|
| `customers` | customer_id, customer_unique_id, customer_city, customer_state |
| `orders` | order_id, customer_id, order_status, all order timestamps |
| `order_items` | order_id, product_id, seller_id, price, freight_value |
| `products` | product_id, product_category_name, dimensions, weight |
| `sellers` | seller_id, seller_city, seller_state |

### Silver (cleaned)
| Table | Key Columns |
|---|---|
| `customers` | customer_id, customer_zip_code, customer_city, customer_state |
| `orders` | order_id, customer_id, purchase_timestamp |
| `order_items` | order_id, product_id, seller_id, price, freight_value |
| `products` | product_id, product_category_name, dimensions, weight |
| `sellers` | seller_id, seller_zip_code, seller_city, seller_state |

### Gold (analytics-ready)
| Table | Description |
|---|---|
| `fact_sales` | order_id, customer_id, seller_id, product_id, purchase_timestamp, price, freight_value |
| `dim_customers` | Customer dimension |
| `dim_products` | Product dimension |
| `dim_sellers` | Seller dimension |

---

## Prerequisites

- Docker Desktop
- PostgreSQL running on port `5433` with databases: `sales_oltp`, `sales_bronze`, `sales_silver`, `sales_gold`

---

## Getting Started

**1. Clone the repository**
```bash
git clone https://github.com/marouanebatrone/ecom-medallion-architecture
cd data-pipeline
```

**2. Configure environment**

Create a `.env` file in the project root:
```
AIRFLOW_UID=50000
_PIP_ADDITIONAL_REQUIREMENTS=
```


**3. Build and start Airflow**
```bash
docker compose build   # builds custom image with Java + PySpark
docker compose up -d
```

**4. Access the Airflow UI**

Navigate to `http://localhost:8080` and log in with `airflow / airflow`.

**5. Drop CSV files and trigger the DAG**

Place the dataset CSV files into `data/input/unprocessed/` and trigger the `medallion_pipeline_dag` DAG from the UI.

---

## DAG Overview

The DAG `medallion_pipeline_dag` runs daily at **00:00 UTC** and consists of 4 sequential tasks:
```
ingest_to_oltp >> oltp_to_bronze >> bronze_to_silver >> silver_to_gold
```

| Task | Description |
|---|---|
| `ingest_to_oltp` | Reads CSVs, casts types, writes to `sales_oltp`. Skips all downstream tasks if no files found. |
| `oltp_to_bronze` | Copies today's records from OLTP to `sales_bronze` |
| `bronze_to_silver` | Cleans, casts, deduplicates into `sales_silver` |
| `silver_to_gold` | Joins orders + items into fact table and writes dimension tables to `sales_gold` |

---

## Running Manually

You can also run each stage independently outside of Airflow:
```bash
python run_pipeline.py ingest
python run_pipeline.py bronze
python run_pipeline.py silver
python run_pipeline.py gold
```

---

## Tech Stack

| Tool | Version | Purpose |
|---|---|---|
| PySpark | 3.3.2 | Data processing |
| PostgreSQL | 18 | Data storage |
| Apache Airflow | 3.1.7 | Orchestration |
| Docker | - | Containerization |
| Java (JDK) | 17 | PySpark runtime |
| PostgreSQL JDBC | 42.7.3 | Spark-Postgres connectivity |