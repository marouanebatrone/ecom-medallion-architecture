from config import DatabaseConfig

class TestFetcher:
    def __init__(self, spark):
        self.spark = spark

    def get_active_observers(self):
        print("Fetching active observers from the registry...")
        query = "(SELECT * FROM observers WHERE is_active = true) as t"
        return self.spark.read.jdbc(DatabaseConfig.OBSERVERS_DB, query, properties=DatabaseConfig.PROPERTIES).collect()