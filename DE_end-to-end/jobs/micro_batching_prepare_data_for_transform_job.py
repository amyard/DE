import argparse
import logging
from datetime import datetime, timedelta

import requests
import psycopg2
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, split, hour, when, udf
from pyspark.sql.types import IntegerType, StringType
from user_agents import parse

# Constants
LOG_FILE = 'script_log.log'
SPARK_APP_NAME = "Spark DataPipeline Prepare Data for Transform"
POSTGRES_DRIVER = "org.postgresql.Driver"
POSTGRES_TABLE = "public.logs_transformed"

# Logging configuration
logging.basicConfig(filename=LOG_FILE, level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Argument parser
parser = argparse.ArgumentParser()
parser.add_argument('--POSTGRES_TABLE', required=True)
parser.add_argument('--POSTGRES_LOGIN', required=True)
parser.add_argument('--POSTGRES_PASSWORD', required=True)
parser.add_argument('--POSTGRES_HOST', required=True)
parser.add_argument('--POSTGRES_PORT', required=True)
parser.add_argument('--POSTGRES_DBNAME', required=True)

args = parser.parse_args()

# PostgreSQL parameters
postgres_params = {
    "url": f"jdbc:postgresql://{args.POSTGRES_HOST}:{args.POSTGRES_PORT}/{args.POSTGRES_DBNAME}",
    "user": args.POSTGRES_LOGIN,
    "password": args.POSTGRES_PASSWORD,
    "driver": POSTGRES_DRIVER
}


# UDFs
@udf(returnType=StringType())
def get_location_by_ip(ip_address):
    try:
        response = requests.get(f"http://ipapi.co/{ip_address}/json/")
        data = response.json()

        if response.status_code == 200:
            return f'{data.get("country_name", "Unknown")}, {data.get("city", "Unknown")}, {data.get("region", "Unknown")}'
        else:
            logging.error(f"Error retrieving data for IP: {ip_address}")
            return 'Unknown, Unknown, Unknown'

    except Exception as e:
        logging.error(f"An error occurred: {e}")
        return 'Unknown, Unknown, Unknown'

@udf(returnType=StringType())
def categorize_age_group(age: int) -> str:
    if not age: return 'None'

    if age >= 60: return '60+'
    elif age >= 41: return '41-60'
    elif age >= 26: return '26-40'
    elif age >= 18: return '18-25'

    return '1-18'

@udf(returnType=StringType())
def extract_log_info(user_agent_str):
    user_agent = parse(user_agent_str)
    device_type = "Tablet" if user_agent.is_tablet else ("Mobile" if user_agent.is_mobile else "Laptop")
    os_family = user_agent.os.family
    browser_family = user_agent.browser.family

    return f"{device_type}, {os_family}, {browser_family}"

def start_spark_session() -> SparkSession:
    """Initialize a Spark session."""
    try:
        spark = (SparkSession.builder
                .appName("Spark DataPipeline Prepare Data for Transform")
                .config("spark.streaming.stopGracefullyOnShutdown", True)
                .config("spark.sql.streaming.stateStore.maintenanceInterval", "30s")
                .getOrCreate()
        )

        return spark
    except Exception as e:
        logging.error("Error starting Spark session: %s", e)
        raise


def load_data(spark: SparkSession):
    """Load data from PostgreSQL into a DataFrame."""
    query = f"(SELECT * FROM logs WHERE status = 'uploaded' AND DATE(created_at) = '{datetime.now().strftime('%Y-%m-%d')}') AS temp_table"
    # query = f"(SELECT * FROM logs WHERE DATE(created_at) = '{(datetime.now() - timedelta(days = 1)).strftime('%Y-%m-%d')}') AS temp_table"

    return (spark.read
            .format("jdbc")
            .option("url", postgres_params["url"])
            .option("dbtable", query)
            .option("user", postgres_params["user"])
            .option("password", postgres_params["password"])
            .option("driver", postgres_params["driver"])
            .load()
    )

def transform_data(df):
    """Transform the DataFrame."""
    df = (df
          .withColumn('full_address_by_ip', get_location_by_ip(col('ip_address')))
          .withColumn('country', split(col('full_address_by_ip'), ',')[0])
          .withColumn('city', split(col('full_address_by_ip'), ',')[1])
          .withColumn('age_group', categorize_age_group(col('age')))
          .withColumn("hours", hour(col("activity_time")).cast(IntegerType()))
          .withColumn("time_of_day",
                      when(col("hours").between(5, 11), "morning")
                      .when(col("hours").between(12, 17), "afternoon")
                      .when(col("hours").between(18, 22), "evening")
                      .otherwise("night"))
          .withColumn('parsed_log_info', extract_log_info(col('log')))
          .withColumn('device_type', split(col('parsed_log_info'), ',')[0])
          .withColumn('os_family', split(col('parsed_log_info'), ',')[1])
          .withColumn('browser_family', split(col('parsed_log_info'), ',')[2])
          .drop('full_address_by_ip', 'log_info', 'parsed_log_info', 'created_at', 'status', 'username', 'id')
    )
    return df

def save_data_into_transformed_table(df):
    """Save the transformed DataFrame to PostgreSQL."""
    df.write \
        .format("jdbc") \
        .option("url", postgres_params["url"]) \
        .option("dbtable", POSTGRES_TABLE) \
        .option("user", postgres_params["user"]) \
        .option("password", postgres_params["password"]) \
        .option("driver", postgres_params["driver"]) \
        .mode("append") \
        .save()

def update_status_in_logs_table(ids):
    """Update the status of processed records in PostgreSQL."""
    batch_size = 100
    ids_batches = [ids[i:i + batch_size] for i in range(0, len(ids), batch_size)]

    for batch_id, batch in enumerate(ids_batches):
        print(f'------- BATCH ID FOR INSERTING  {batch_id}')
        ids_str = ','.join([f"'{id}'" for id in batch])
        update_query = f"""
                UPDATE logs
                SET status = 'transformed'
                WHERE id IN ({ids_str})
            """

        try:
            with psycopg2.connect(**{
                "host": args.POSTGRES_HOST,
                "port": args.POSTGRES_PORT,
                "database": args.POSTGRES_DBNAME,
                "user": args.POSTGRES_LOGIN,
                "password": args.POSTGRES_PASSWORD
            }) as connection:
                with connection.cursor() as cursor:
                    cursor.execute(update_query)
                    connection.commit()
                    logging.info(f"Updated status for IDs: {ids_str}")

        except Exception as e:
            logging.error(f"Error while updating status: {e}")

def main():
    """Main function to execute the pipeline."""
    spark = start_spark_session()

    df = load_data(spark).cache()
    ids = [row['id'] for row in df.select(col('id')).collect()]
    df = transform_data(df)
    print(f'---------  AMOUT OF DATA   {df.count()}')

    # Save transformed data
    logging.info('Starting to save transformed data...')
    save_data_into_transformed_table(df)

    # Update status
    logging.info('Starting status update...')
    update_status_in_logs_table(ids)

    spark.stop()

if __name__ == "__main__":
    main()