import argparse
import logging

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.connect.dataframe import DataFrame
from pyspark.sql.functions import col, when
from pyspark.sql.functions import desc, count, sum, rank, lag, round, year, month
from pyspark.sql.window import Window

# Constants
LOG_FILE = 'script_log.log'
SPARK_APP_NAME = "Spark DataPipeline Transformation"
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
    return (spark.read
            .format("jdbc")
            .option("url", postgres_params["url"])
            .option("dbtable", 'public.logs_transformed')
            .option("user", postgres_params["user"])
            .option("password", postgres_params["password"])
            .option("driver", postgres_params["driver"])
            .load()
    )

def save_data_into_transformed_table(df: DataFrame, table_name: str):
    """Save DataFrame to PostgreSQL."""
    logging.info(f"Save DataFrame '{table_name}' to PostgreSQL.")
    df.write \
        .format("jdbc") \
        .option("url", postgres_params["url"]) \
        .option("dbtable", f"public.{table_name}") \
        .option("user", postgres_params["user"]) \
        .option("password", postgres_params["password"]) \
        .option("driver", postgres_params["driver"]) \
        .mode("overwrite") \
        .save()

def transform_by_one_column(df: DataFrame, column_name: str) -> DataFrame:
    return (df
             .groupBy(column_name).agg(count(column_name).alias('amount'))
             .withColumn('rank', rank().over(Window.orderBy(desc(col('amount')))))
    )

def transform_by_age_group(df: DataFrame) -> DataFrame:
    return (df
             .groupBy(col('age_group'), col('time_of_day')).agg(count('age_group').alias('amount'))
             .withColumn('sort_cond_1',
                            when(col('age_group') == '1-18', 1)
                            .when(col('age_group') == '18-25', 2)
                            .when(col('age_group') == '26-40', 3)
                            .when(col('age_group') == '41-60', 4)
                            .when(col('age_group') == '60+', 5)
                            .otherwise(6)
                         )
             .withColumn('sort_cond_2',
                            when(col('time_of_day') == 'morning', 1)
                            .when(col('time_of_day') == 'afternoon', 2)
                            .when(col('time_of_day') == 'evening', 3)
                            .when(col('time_of_day') == 'night', 4)
                            .otherwise(5)
                         )
             .sort('sort_cond_1', 'sort_cond_2')
             .select(col('age_group'), col('time_of_day'), col('amount'))
    )

def transform_by_year_and_month(df: DataFrame) -> DataFrame:
    return (df
         .withColumn('years', year(col('activity_time')))
         .withColumn('month', month(col('activity_time')))
         .groupBy('years', 'month')
            .agg(count('years').alias('current_period_amount'))
         .withColumn('previous_period_amount', lag(col('current_period_amount'), 1, 0).over(Window.orderBy(col('years'), col('month'))))
         .withColumn('increase', col('current_period_amount') - col('previous_period_amount'))
         .withColumn('clicks_increase', when(col('increase') > 0, True).otherwise(False))
         .withColumn('year_sum', sum(col('current_period_amount')).over(Window.orderBy(col('years'))))
         .withColumn('percentage_per_year', round(col('current_period_amount')/col('year_sum') * 100, 2))
    )

def main():
    """Main function to execute the pipeline."""
    spark: SparkSession = start_spark_session()

    df: DataFrame = load_data(spark).cache()

    # Transform df's
    df_county: DataFrame = transform_by_one_column(df, 'country')
    df_city: DataFrame = transform_by_one_column(df, 'city')
    df_age_group: DataFrame = transform_by_age_group(df)
    df_year_month: DataFrame = transform_by_age_group(df)

    # save into Postgres
    save_data_into_transformed_table(df_county, 'df_county')
    save_data_into_transformed_table(df_city, 'df_city')
    save_data_into_transformed_table(df_age_group, 'df_age_group')
    save_data_into_transformed_table(df_year_month, 'df_year_month')

    spark.stop()

if __name__ == "__main__":
    main()