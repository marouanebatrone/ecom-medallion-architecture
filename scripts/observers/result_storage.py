import os
import json
import uuid
import psycopg2
from pyspark.sql.types import StructType, StructField, StringType, FloatType, TimestampType
from config import DatabaseConfig

class ResultStorage:
    def __init__(self, spark):
        self.spark = spark
        self.schema = StructType([
            StructField("run_id",           StringType(),   False),
            StructField("observer_id",      StringType(),   False),
            StructField("execution_time",   TimestampType(), False),
            StructField("metric_value",     FloatType(),    True),
            StructField("threshold_value",  FloatType(),    True),
            StructField("status",           StringType(),   False),
            StructField("created_at",       TimestampType(), False)
        ])

    def save_results(self, results):
        if not results:
            return

        print(f"Persisting {len(results)} execution runs...")

        temp_path = f"/tmp/observer_results_{uuid.uuid4().hex}.json"
        try:
            with open(temp_path, "w") as f:
                for item in results:
                    f.write(json.dumps(item) + "\n")

            self._upsert_results(results)

        finally:
            if os.path.exists(temp_path):
                os.remove(temp_path)

    def _upsert_results(self, results):
        conn = psycopg2.connect(
            host="host.docker.internal",
            port=5433,
            dbname="observers_db",
            user="postgres",
            password="admin"
        )
        try:
            with conn.cursor() as cur:
                for item in results:
                    cur.execute(
                        """
                        INSERT INTO observer_runs
                            (run_id, observer_id, execution_time,
                             metric_value, threshold_value, status, created_at)
                        VALUES (%s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (run_id) DO NOTHING
                        """,
                        (
                            item["run_id"],
                            item["observer_id"],
                            item["execution_time"],
                            item["metric_value"],
                            item["threshold_value"],
                            item["status"],
                            item["created_at"],
                        )
                    )
            conn.commit()
            print(f"Successfully upserted {len(results)} rows into observer_runs.")
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()