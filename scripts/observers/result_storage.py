import os
import json
from pyspark.sql.types import StructType, StructField, StringType, FloatType, TimestampType
from config import DatabaseConfig

class ResultStorage:
    def __init__(self, spark):
        self.spark = spark
        self.schema = StructType([
            StructField("run_id", StringType(), False),
            StructField("observer_id", StringType(), False),
            StructField("execution_time", TimestampType(), False),
            StructField("metric_value", FloatType(), True),
            StructField("threshold_value", FloatType(), True),
            StructField("status", StringType(), False),
            StructField("created_at", TimestampType(), False)
        ])

    def save_results(self, results):
        if not results: return
        
        print(f"Persisting {len(results)} execution runs...")
        temp_path = "/tmp/temp_results.json"
        with open(temp_path, "w") as f:
            for item in results:
                f.write(json.dumps(item) + "\n")

        df = self.spark.read.json(temp_path, schema=self.schema)
        df.write.jdbc(DatabaseConfig.OBSERVERS_DB, "observer_runs", "append", DatabaseConfig.PROPERTIES)
        os.remove(temp_path)