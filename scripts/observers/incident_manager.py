from pyspark.sql import functions as F
from pyspark.sql.types import TimestampType, StringType
from config import DatabaseConfig

class IncidentManager:
    def __init__(self, spark):
        self.spark = spark

    def log_new_incidents(self):
        print("Scanning for failed runs to generate incidents...")
        query = """(SELECT run_id, observer_id, execution_time, status, created_at 
                   FROM observer_runs 
                   WHERE status = 'fail' AND DATE(created_at) = CURRENT_DATE) as t"""
        
        failures = self.spark.read.jdbc(DatabaseConfig.OBSERVERS_DB, query, properties=DatabaseConfig.PROPERTIES)
        
        if failures.count() > 0:
            incidents = failures.select(
                F.expr("uuid()").alias("incident_id"),
                F.col("observer_id"),
                F.col("run_id"),
                F.col("status"),
                F.col("execution_time").alias("detected_at"), 
                F.lit(None).cast(TimestampType()).alias("resolved_at"),
                F.lit(None).cast(StringType()).alias("root_cause"),
                F.lit(None).cast(StringType()).alias("resolution_notes"),
                F.col("created_at")
            )
            incidents.write.jdbc(DatabaseConfig.OBSERVERS_DB, "incidents", "append", DatabaseConfig.PROPERTIES)
            print(f"Logged {failures.count()} incidents successfully.")