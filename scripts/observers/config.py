from pyspark.sql import SparkSession

class SparkConfig:
    @staticmethod
    def get_session():
        spark = SparkSession.builder \
            .appName("QupidObserverPipeline") \
            .config("spark.jars.packages", "org.postgresql:postgresql:42.7.3") \
            .getOrCreate()
        spark.sparkContext.setLogLevel("ERROR")
        return spark

class DatabaseConfig:
    TARGET_DB = "jdbc:postgresql://host.docker.internal:5433/sales_data_platform"
    OBSERVERS_DB = "jdbc:postgresql://host.docker.internal:5433/observers_db"
    
    PROPERTIES = {
        "user": "postgres",
        "password": "admin",
        "driver": "org.postgresql.Driver",
        "stringtype": "unspecified"
    }