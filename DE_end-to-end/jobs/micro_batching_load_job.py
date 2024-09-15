import os
# os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.2,org.postgresql:postgresql:42.7.3,org.apache.hadoop:hadoop-azure:3.4.0,com.azure:azure-storage-blob:12.27.1'

import argparse
import logging
from datetime import datetime
from typing import Dict

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import IntegerType, TimestampType, StringType, StructType, StructField
from azure.storage.blob import BlobServiceClient

log_file = 'script_log.log'
logging.basicConfig(filename=log_file, level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


parser = argparse.ArgumentParser()
parser.add_argument('--KAFKA_BOOTSTRAP_SERVER', required=True)
parser.add_argument('--KAFKA_TOPIC', required=True)
parser.add_argument('--AZURE_CONTAINER_NAME', required=True)
parser.add_argument('--AZURE_STORAGE_ACCOUNT_NAME', required=True)
parser.add_argument('--AZURE_STORAGE_ACCOUNT_KEY', required=True)
parser.add_argument('--POSTGRES_TABLE', required=True)
parser.add_argument('--POSTGRES_LOGIN', required=True)
parser.add_argument('--POSTGRES_PASSWORD', required=True)
parser.add_argument('--POSTGRES_HOST', required=True)
parser.add_argument('--POSTGRES_PORT', required=True)
parser.add_argument('--POSTGRES_DBNAME', required=True)

args = parser.parse_args()
kafka_bootstrap_server = args.KAFKA_BOOTSTRAP_SERVER
kafka_topic = args.KAFKA_TOPIC

azure_container_name = args.AZURE_CONTAINER_NAME
azure_storage_account_name = args.AZURE_STORAGE_ACCOUNT_NAME
azure_storage_account_key = args.AZURE_STORAGE_ACCOUNT_KEY
storage_url = f"wasbs://{azure_container_name}@{azure_storage_account_name}.blob.core.windows.net"

postgres_table_name = args.POSTGRES_TABLE
postgres_login = args.POSTGRES_LOGIN
postgres_password = args.POSTGRES_PASSWORD
postgres_host = args.POSTGRES_HOST
postgres_port = args.POSTGRES_PORT
postgres_dbname = args.POSTGRES_DBNAME


def start_session() -> SparkSession:
    try:
        """Start a PySpark session."""

        spark = (
            SparkSession.builder
                .appName("Spark DataPipeline")
                .config("spark.streaming.stopGracefullyOnShutdown", True)
                # .config('spark.jars.packages', "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.2,org.postgresql:postgresql:42.7.3,org.apache.hadoop:hadoop-azure:3.4.0,com.azure:azure-storage-blob:12.27.1")
                .config("spark.sql.streaming.stateStore.maintenanceInterval", "30s")
                .getOrCreate()
        )

        return spark
    except Exception as e:
        print(e)
        return None

spark = start_session()


def get_data_from_kafka() -> DataFrame:
    try:
        streaming_df: DataFrame = (
            spark.readStream
                .format("kafka")
                .option("kafka.bootstrap.servers", kafka_bootstrap_server)
                .option("subscribe", kafka_topic)
                .option("startingOffsets", "earliest")
                # .option("fetchOffset.retry.timeout", "30000")
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
streamed_df = (df
        .selectExpr("CAST(value AS STRING)")
        .select(from_json(col('value'), json_schema).alias('data'))
        .select("data.*"))

spark.conf.set(f"fs.azure.account.key.{azure_storage_account_name}.blob.core.windows.net", azure_storage_account_key)
azure_blob_path = f"input_{datetime.now().strftime('%Y-%m-%d')}"

def write_to(batch_df, batch_id):
    import random
    rand_int: int = random.randint(1, 10)

    if batch_id == 0:
        write_to_blob_as_JSON(batch_df, batch_id)
        return

    if rand_int == 1:
        write_to_blob_as_JSON(batch_df, batch_id)
    elif rand_int == 2:
        write_to_blob_as_CSV(batch_df, batch_id)
    elif rand_int == 3:
        write_to_blob_as_PARQUET(batch_df, batch_id)
    else:
        write_to_DB(batch_df, batch_id)


def write_to_DB(batch_df, batch_id):
    url = f"jdbc:postgresql://{postgres_host}:{postgres_port}/{postgres_dbname}"
    properties = {
        "user": postgres_login,
        "password": postgres_password,
        "driver": "org.postgresql.Driver",
        "batchSize": "100",
        "socketTimeout": "60000",
        "connectionTimeout": "60000"
    }

    try:
        logging.info(f"------------Writing batch {batch_id} to table {postgres_table_name}---------------")
        batch_df.write.jdbc(url=url, table=postgres_table_name, mode="append", properties=properties)
    except Exception as e:
        logging.error("Error writing to PostgreSQL: %s", e)
        raise

def write_to_blob_as_PARQUET(batch_df, batch_id):
    logging.info(f"-------    write_to_blob_as_PARQUET   {batch_id}  -------")
    output_path = f"{storage_url}/{azure_blob_path}/batch_full.parquet"
    batch_df.coalesce(1).write.mode("append").parquet(output_path)

def write_to_blob_as_CSV(batch_df: DataFrame, batch_id):
    logging.info(f"-------    write_to_blob_as_CSV   {batch_id}  -------")
    batch_df.persist()
    _options: Dict[str, str] = {
        "header": "true",
        "inferSchema": "true",
        "delimiter": "|"
    }
    output_path = f"{storage_url}/{azure_blob_path}/batch_1_{batch_id}.csv"
    batch_df.coalesce(1).write.options(**_options).mode("append").csv(output_path)
    batch_df.unpersist()

def write_to_blob_as_JSON(batch_df, batch_id):
    logging.info(f"-------    write_to_blob_as_JSON   {batch_id}  -------")
    json_data = batch_df.toJSON().collect()
    blob_service_client = BlobServiceClient(
        account_url=f"https://{azure_storage_account_name}.blob.core.windows.net",
        credential=azure_storage_account_key)
    blob_client = blob_service_client.get_blob_client(
        container=azure_container_name,
        blob=f"{azure_blob_path}/batch_{batch_id}.json")
    blob_client.upload_blob("\n".join(json_data), overwrite=True)

query = (streamed_df
            .writeStream
            .trigger(processingTime='30 seconds')
            # .option("checkpointLocation", os.path.join(os.path.dirname(os.path.abspath(__file__)), "tmp"))
            .foreachBatch(write_to_DB)
            .outputMode("append")
            .start())

# query = sel \
#     .writeStream \
#     .trigger(processingTime='1 seconds') \
#     .outputMode("update") \
#     .option("truncate", "false")\
#     .format("console") \
#     .start()

query.awaitTermination()

spark.stop()