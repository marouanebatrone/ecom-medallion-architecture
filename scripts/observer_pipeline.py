from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, FloatType, TimestampType
import pandas as pd
import json
import os
import uuid
from datetime import datetime, timezone
from pyspark.sql import functions as F

class ObserverPipeline:
    def __init__(self):
        self.spark = SparkSession.builder \
            .appName("QupidObserverPipeline") \
            .config("spark.jars.packages", "org.postgresql:postgresql:42.7.3") \
            .getOrCreate()
        
        self.spark.sparkContext.setLogLevel("ERROR")

        # Database connection properties
        self.jdbc_url = "jdbc:postgresql://host.docker.internal:5433/sales_data_platform"
        self.observers_db = "jdbc:postgresql://host.docker.internal:5433/observers_db"
        
        self.properties = {
            "user": "postgres",
            "password": "admin",
            "driver": "org.postgresql.Driver",
            "stringtype": "unspecified"
        }

    # METHOD 1: Fetch active tests
    def fetch_tests(self):
        print("Fetching active observers...")
        df = self.spark.read.jdbc(
            self.observers_db, 
            "(SELECT * FROM observers WHERE is_active = true) as t", 
            properties=self.properties
        )
        return df.collect()

    # METHOD 2 & 3: Build/Run queries and Store Results
    def run_and_store_tests(self, active_tests):
        print(f"Running {len(active_tests)} tests...")
        
        run_results = []
        now = datetime.now(timezone.utc)

        for test in active_tests:
            config = json.loads(test.condition_config) if test.condition_config else {}
            full_table = f"{test.db_name}.{test.schema_name}.{test.table_name}"
            
            run_id = str(uuid.uuid4())
            metric_val = None
            threshold_val = None
            status = "fail"

            try:
                # --- LOGIC: NULLS ---
                if test.observer_type == "nulls":
                    threshold_val = float(config.get("threshold", 0))
                    operator = config.get("operator", "<=")

                    if test.column_name:
                        query = f"(SELECT SUM(CASE WHEN {test.column_name} IS NULL THEN 1.0 ELSE 0.0 END) / COUNT(*) as val FROM {full_table}) as t"
                    else:
                        # Fallback for whole table: proxy by checking if any row is entirely null (adjust as needed)
                        query = f"(SELECT 0.0 as val FROM {full_table} LIMIT 1) as t" 

                    res = self.spark.read.jdbc(self.jdbc_url, query, properties=self.properties).collect()[0]
                    metric_val = float(res["val"])
                    status = self._evaluate(metric_val, operator, threshold_val)

                # --- LOGIC: UNIQUENESS ---
                elif test.observer_type == "uniqueness":
                    query = f"(SELECT COUNT({test.column_name}) as total, COUNT(DISTINCT {test.column_name}) as distinct_total FROM {full_table}) as t"
                    res = self.spark.read.jdbc(self.jdbc_url, query, properties=self.properties).collect()[0]
                    status = "pass" if res["total"] == res["distinct_total"] else "fail"

                # --- LOGIC: SCHEMA ---
                elif test.observer_type == "schema":
                    real_schema = test.schema_name
                    real_table = test.table_name
                    real_schema = real_schema.lower()
                    real_table = real_table.lower()
                    if test.column_name:
                        real_col = str(test.column_name).lower()
                        query = f"""(
                            SELECT COUNT(*) as cnt 
                            FROM information_schema.columns 
                            WHERE LOWER(table_schema) = '{real_schema}' 
                            AND LOWER(table_name) = '{real_table}' 
                            AND LOWER(column_name) = '{real_col}'
                        ) as t"""
                    else:
                        query = f"""(
                            SELECT COUNT(*) as cnt 
                            FROM information_schema.tables 
                            WHERE LOWER(table_schema) = '{real_schema}' 
                            AND LOWER(table_name) = '{real_table}'
                        ) as t"""
                    # 3. Execute and evaluate
                    res = self.spark.read.jdbc(self.jdbc_url, query, properties=self.properties).collect()[0]
                    status = "pass" if res["cnt"] > 0 else "fail"
                    if status == "fail":
                        print(f"[DEBUG] Existence check failed for {real_schema}.{real_table}")

                # --- LOGIC: SIZE ---
                elif test.observer_type == "size":
                    threshold_val = float(config.get("threshold", config.get("min_rows", 0)))
                    operator = config.get("operator", ">=")
                    date_col = config.get("column", "date_ingested")
                    
                    query = f"(SELECT COUNT(*) as val FROM {full_table} WHERE DATE({date_col}) = CURRENT_DATE) as t"
                    res = self.spark.read.jdbc(self.jdbc_url, query, properties=self.properties).collect()[0]
                    metric_val = float(res["val"])
                    status = self._evaluate(metric_val, operator, threshold_val)

            except Exception as e:
                print(f"[ERROR] Test {test.observer_name} failed to execute: {e}")
                status = "error"

            # Append to results
            run_results.append((
                run_id, test.observer_id, now, metric_val, threshold_val, status, now
            ))

        # Store in observer_runs
        if run_results:
            
            schema = StructType([
            StructField("run_id", StringType(), False),
            StructField("observer_id", StringType(), False),
            StructField("execution_time", TimestampType(), False),
            StructField("metric_value", FloatType(), True),
            StructField("threshold_value", FloatType(), True),
            StructField("status", StringType(), False),
            StructField("created_at", TimestampType(), False)
        ])
            
            # 2. Convert datetime objects to strings for JSON compatibility
            json_ready_results = []
            for r in run_results:
                res_dict = {
                    "run_id": r[0],
                    "observer_id": r[1],
                    "execution_time": r[2].isoformat(),
                    "metric_value": r[3],
                    "threshold_value": r[4],
                    "status": r[5],
                    "created_at": r[6].isoformat()
                }
                json_ready_results.append(res_dict)

            # 3. Save to a temporary file
            temp_path = "/tmp/temp_results.json"
            with open(temp_path, "w") as f:
                for item in json_ready_results:
                    f.write(json.dumps(item) + "\n")

            # 4. Read into Spark (this avoids the Pickle Error!)
            df_runs = self.spark.read.json(temp_path, schema=schema)
            
            # 5. Write to DB and clean up
            df_runs.write.jdbc(self.observers_db, "observer_runs", "append", self.properties)
            os.remove(temp_path)
            print(f"Stored {len(run_results)} runs successfully.")
            
                       

    # METHOD 4: Log Incidents
    
    def log_incidents(self):
        print("Scanning for today's incidents...")
        
        # 1. Fetch only the failures from the runs table
        # We only need the columns required for the incidents table
        query = """
        (SELECT run_id, observer_id, execution_time, status, created_at 
         FROM observer_runs 
         WHERE status = 'fail' AND DATE(created_at) = CURRENT_DATE) as t
        """
        failed_runs_df = self.spark.read.jdbc(self.observers_db, query, properties=self.properties)
        
        incident_count = failed_runs_df.count()
        
        if incident_count > 0:
            print(f"Transforming {incident_count} failures into incidents...")
            
            # 2. Map the data to the 'incidents' table schema exactly
            incidents_to_write = failed_runs_df.select(
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
            
            # 3. Write once to the incidents table
            incidents_to_write.write.jdbc(
                self.observers_db, 
                "incidents", 
                "append", 
                self.properties
            )
            print(f"Successfully logged {incident_count} incidents.")
        else:
            print("No incidents detected today. Pipeline healthy.")
            
            
    # Helper function for evaluation
    def _evaluate(self, metric, operator, threshold):
        if operator == "<=": return "pass" if metric <= threshold else "fail"
        if operator == ">=": return "pass" if metric >= threshold else "fail"
        if operator == "=" or operator == "==": return "pass" if metric == threshold else "fail"
        if operator == "<": return "pass" if metric < threshold else "fail"
        if operator == ">": return "pass" if metric > threshold else "fail"
        return "fail"

if __name__ == "__main__":
    pipeline = ObserverPipeline()
    tests = pipeline.fetch_tests()
    if tests:
        pipeline.run_and_store_tests(tests)
        pipeline.log_incidents()
    else:
        print("No active tests found.")