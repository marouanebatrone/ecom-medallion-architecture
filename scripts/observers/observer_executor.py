import json
from config import DatabaseConfig
from test_evaluator import TestEvaluator

class ObserverExecutor:
    def __init__(self, spark):
        self.spark = spark
        self.evaluator = TestEvaluator()

    def run_observer(self, test):
        config = json.loads(test.condition_config) if test.condition_config else {}
        table_path = f"{test.db_name}.{test.schema_name}.{test.table_name}"
        
        if test.observer_type == "nulls":
            return self._execute_null_check(test, table_path, config)
        elif test.observer_type == "uniqueness":
            return self._execute_uniqueness_check(test, table_path)
        elif test.observer_type == "schema":
            return self._execute_schema_check(test)
        elif test.observer_type == "size":
            return self._execute_size_check(test, table_path, config)
        
        return None, None, "error"

    def _execute_null_check(self, test, table, config):
        query = f"(SELECT SUM(CASE WHEN {test.column_name} IS NULL THEN 1.0 ELSE 0.0 END) / COUNT(*) as val FROM {table}) as t"
        metric = float(self._fetch_metric(query))
        status = self.evaluator.evaluate_condition(metric, config.get("operator", "<="), float(config.get("threshold", 0)))
        return metric, float(config.get("threshold", 0)), status

    def _execute_uniqueness_check(self, test, table):
        query = f"(SELECT COUNT({test.column_name}) as total, COUNT(DISTINCT {test.column_name}) as distinct_total FROM {table}) as t"
        res = self.spark.read.jdbc(DatabaseConfig.TARGET_DB, query, properties=DatabaseConfig.PROPERTIES).collect()[0]
        status = "pass" if res["total"] == res["distinct_total"] else "fail"
        return None, None, status

    def _execute_size_check(self, test, table, config):
        date_col = config.get("column", "date_ingested")
        query = f"(SELECT COUNT(*) as val FROM {table} WHERE DATE({date_col}) = CURRENT_DATE) as t"
        metric = float(self._fetch_metric(query))
        threshold = float(config.get("threshold", config.get("min_rows", 0)))
        status = self.evaluator.evaluate_condition(metric, config.get("operator", ">="), threshold)
        return metric, threshold, status

    def _fetch_metric(self, query):
        return self.spark.read.jdbc(DatabaseConfig.TARGET_DB, query, properties=DatabaseConfig.PROPERTIES).collect()[0]["val"]

    def _execute_schema_check(self, test):
        schema_name = str(test.schema_name).lower()
        table_name = str(test.table_name).lower()
        
        if test.column_name:
            column_name = str(test.column_name).lower()
            query = f"""(
                SELECT COUNT(*) as cnt 
                FROM information_schema.columns 
                WHERE LOWER(table_schema) = '{schema_name}' 
                AND LOWER(table_name) = '{table_name}' 
                AND LOWER(column_name) = '{column_name}'
            ) as t"""
        else:
            query = f"""(
                SELECT COUNT(*) as cnt 
                FROM information_schema.tables 
                WHERE LOWER(table_schema) = '{schema_name}' 
                AND LOWER(table_name) = '{table_name}'
            ) as t"""

        result = self.spark.read.jdbc(DatabaseConfig.TARGET_DB, query, properties=DatabaseConfig.PROPERTIES).collect()[0]

        metric = float(result["cnt"])
        threshold = 1.0
        status = "pass" if metric >= threshold else "fail"

        if status == "fail":
            target_description = f"{schema_name}.{table_name}"
            if test.column_name:
                target_description += f" (column: {test.column_name})"
            print(f"[DEBUG] Schema check failed: Resource '{target_description}' was not found.")

        return metric, threshold, status