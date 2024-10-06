import logging
from pyspark.sql.context import SparkSession, DataFrame


class SparkHelper:
    def __init__(self):
        self.spark: SparkSession = self.__start_spark_session()

    def __start_spark_session(self) -> SparkSession:
        """Initialize a Spark session."""
        try:
            spark: SparkSession = (SparkSession.builder
                                     .appName("Spark DataPipeline")
                                     .config("spark.streaming.stopGracefullyOnShutdown", True)
                                     .config("spark.sql.streaming.stateStore.maintenanceInterval", "30s")
                                     .getOrCreate()
                                     )

            return spark
        except Exception as e:
            logging.error("Error starting Spark session: %s", e)
            return None


class PostgresHelper:
    def __init__(self, url, database, user, password, driver):
        self.url = url
        self.database = database
        self.user = user
        self.password = password
        self.driver = driver

    def load_data(self, spark: SparkSession, table_name: str):
        """Load data from PostgreSQL into a DataFrame."""
        return (spark.read
                .format("jdbc")
                .option("url", self.url)
                .option("dbtable", table_name)
                .option("user", self.user)
                .option("password", self.password)
                .option("driver", self.driver)
                .load()
                )

    def save_data_into_table(self, df: DataFrame, table_name: str):
        """Save DataFrame to PostgreSQL."""
        logging.info(f"Save DataFrame '{table_name}' to PostgreSQL.")
        df.write \
            .format("jdbc") \
            .option("url", self.url) \
            .option("dbtable", f"public.{table_name}") \
            .option("user", self.user) \
            .option("password", self.password) \
            .option("driver", self.driver) \
            .mode("overwrite") \
            .save()
