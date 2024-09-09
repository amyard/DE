import logging
import os
from datetime import datetime
from typing import Dict

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import IntegerType, TimestampType, StringType, DecimalType, BooleanType, StructType, StructField, Row


def start_session(app_name: str = "Spark Data Pipeline", host: str = "local[*]") -> SparkSession:
    try:
        """Start a PySpark session."""
        # findspark.init()
        # findspark.find()

        spark = (
            SparkSession.builder
                    .appName("Spark DataPipeline")
                    .config("spark.streaming.stopGracefullyOnShutdown", True)
                    .config('spark.jars.packages', "org.postgresql:postgresql:42.7.3,org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0,org.apache.hadoop:hadoop-azure:3.4.0,com.azure:azure-storage-blob:12.27.1")
                    # .config("spark.sql.shuffle.partitions", "10")
                    # .config("spark.executor.instances", "2")
                    # .config("spark.executor.cores", "4")
                    # .config("spark.driver.memory", "8g")
                    # .config("spark.executor.memory", "8g")
                    # .config("spark.network.timeout", "600s")
                    # .config("spark.executor.heartbeatInterval", "60s")
                    # .config("spark.rpc.message.maxSize", "800")
                    # .config("spark.local.dir", os.path.join(os.path.dirname(os.path.abspath(__file__)), "tmp"))
                    # .master("spark://172.22.0.11:7077")
                    # .master("local[*]")
                    .getOrCreate()
        )

        return spark
    except Exception as e:
        print(e)
        return None

spark = start_session(host = 'spark://spark-master:7077')

print('AAAAAAAAAAAAAAAAAAAA')



KAFKA_BOOTSTRAP_SERVER = 'broker:29092'
KAFKA_TOPIC = 'micro_batching_topic'

def get_data_from_kafka() -> DataFrame:
    try:
        streaming_df: DataFrame = (
            spark.readStream
                .format("kafka")
                # .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVER)
                .option("kafka.bootstrap.servers", 'localhost:9092')
                .option("subscribe", KAFKA_TOPIC)
                .option("startingOffsets", "earliest")
                .load()
        )
        return streaming_df
    except Exception as e:
        logging.error("Error reading from Kafka: %s", e)
        raise


json_schema = StructType([
  StructField("username", StringType()),
  StructField("age", IntegerType()),
  StructField("gender", StringType()),
  StructField("ad_position", StringType()),
  StructField("browsing_history", StringType()),
  StructField("activity_time", TimestampType()),
  StructField("ip_address", StringType()),
  StructField("log", StringType()),
  StructField("redirect_from", StringType()),
  StructField("redirect_to", StringType()),
])


df = get_data_from_kafka()
sel = (df
        .selectExpr("CAST(value AS STRING)")
        .select(from_json(col('value'), json_schema).alias('data'))
        .select("data.*"))


# query = sel.writeStream.outputMode("append").format("console").start()

print("-------------   LOAD FILE  -----------------")

# Define the output location in Azure Blob Storage (replace with your container and folder path)
storage_account_name = 'delmestoragesaccount'
access_key = ''
container = 'dirty'

output_path = f"wasbs://{container}@{storage_account_name}.blob.core.windows.net/input-09_09_2024/"

spark.conf.set(f"fs.azure.account.key.{storage_account_name}.blob.core.windows.net", access_key)

# Write stream to Parquet format in Azure Blob Storage
_options: Dict[str, str] = {
    "header" : "true",
    "inferSchema" : "true",
    "delimiter": "|"
}

query = sel.writeStream \
    .format("csv") \
    .option("path", output_path) \
    .options(**_options) \
    .option("checkpointLocation", f"wasbs://checkpoint@{storage_account_name}.blob.core.windows.net/micro_batching/") \
    .outputMode("append") \
    .start()


query.awaitTermination()

spark.stop()
