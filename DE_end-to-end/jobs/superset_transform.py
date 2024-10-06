import argparse

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, months_between, current_date, date_format, lit, round, udf, to_date, month, row_number, desc, count, sum, hour, rank, month, lag, avg, round
from pyspark.sql.types import IntegerType, TimestampType, StringType, DecimalType, BooleanType, StructType, StructField, Row
from pyspark.sql.window import Window
from helpers import SparkHelper, PostgresHelper


POSTGRES_DRIVER = "org.postgresql.Driver"

parser = argparse.ArgumentParser()
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

@udf(returnType=BooleanType())
def income_more_than_month_avg(avg_income: int, income: int) -> bool:
    return False if avg_income > income else True

def get_total_income_by_hours(orders: DataFrame) -> DataFrame:
    total_income_by_hours: DataFrame = orders \
        .withColumn('hours', hour(col('order_date'))) \
        .groupby('hours').agg(sum('total_price').alias('total_income')) \
        .sort(col('total_income').desc())
    return total_income_by_hours

def get_total_income_by_genders(orders: DataFrame, users: DataFrame) -> DataFrame:
    total_income_by_genders: DataFrame = orders.alias('o')\
        .join(users.alias('u'), on = col('u.email') == col('o.customer_email'))\
        .groupby(col('u.gender')).agg(sum('total_price').alias('total_income'))\
        .withColumn('rank', rank().over(Window.orderBy(col('total_income'))))
    return total_income_by_genders

def get_total_income_by_dates(orders: DataFrame) -> DataFrame:
    total_income_by_dates: DataFrame = orders.groupby(col('date')).agg(sum('total_price').alias('total_income')).sort(col('date'))
    return total_income_by_dates

def get_total_income_by_dates_with_month(orders: DataFrame) -> DataFrame:
    total_income_by_dates_with_month = orders \
        .groupby(col('month'), col('date')).agg(sum('total_price').alias('total_income')).sort(col('date')) \
        .withColumn('prev_income', lag(col('total_income'), 1, 0).over(Window.orderBy(col('month'), col('date')))) \
        .withColumn('profit', col('total_income') - col('prev_income'))
    return total_income_by_dates_with_month

def get_total_income_by_avg_month(orders: DataFrame) -> DataFrame:
    total_income_by_avg_month = orders \
        .groupby(col('month'), col('date')) \
        .agg(sum('total_price').alias('total_income')) \
        .sort(col('date')) \
        .withColumn('avg_month', avg(col('total_income')).over(Window.orderBy(col('month')))) \
        .withColumn('avg_month', round(col('avg_month'), 2)) \
        .withColumn('income_more_than_month_avg', income_more_than_month_avg(col('avg_month'), col('total_income')))
    return total_income_by_avg_month

def get_profit_by_age_group_and_gender(orders: DataFrame, users: DataFrame) -> DataFrame:
    profit_by_age_group_and_gender: DataFrame = orders.alias('o') \
        .join(users.alias('u'), on=col('u.email') == col('o.customer_email')) \
        .groupby(col('u.gender'), col('u.age_group')) \
        .agg(sum('total_price').alias('total_income')) \
        .sort(col('u.gender'), col('u.age_group'))
    return profit_by_age_group_and_gender

def main():
    spark: SparkSession = SparkHelper().spark
    postgres_connection = PostgresHelper(
        url=postgres_params["url"],
        database=args.POSTGRES_DBNAME,
        user=postgres_params["user"],
        password=postgres_params["password"],
        driver=postgres_params["driver"],
    )

    # load data from tables
    users_df: DataFrame = postgres_connection.load_data(spark, "public.cleaned_users")
    orders_df: DataFrame = postgres_connection.load_data(spark, "public.cleaned_orders")
    users_df.cache()
    orders_df.cache()

    # data manipulation
    total_income_by_hours: DataFrame = get_total_income_by_hours(orders_df)
    total_income_by_genders: DataFrame = get_total_income_by_genders(orders_df, users_df)
    total_income_by_dates: DataFrame = get_total_income_by_dates(orders_df)
    total_income_by_dates_with_month: DataFrame = get_total_income_by_dates_with_month(orders_df)
    total_income_by_avg_month: DataFrame = get_total_income_by_avg_month(orders_df)
    profit_by_age_group_and_gender: DataFrame = get_profit_by_age_group_and_gender(orders_df, users_df)

    # save data
    postgres_connection.save_data_into_table(total_income_by_hours, "total_income_by_hours")
    postgres_connection.save_data_into_table(total_income_by_genders, "total_income_by_genders")
    postgres_connection.save_data_into_table(total_income_by_dates, "total_income_by_dates")
    postgres_connection.save_data_into_table(total_income_by_dates_with_month, "total_income_by_dates_with_month")
    postgres_connection.save_data_into_table(total_income_by_avg_month, "total_income_by_avg_month")
    postgres_connection.save_data_into_table(profit_by_age_group_and_gender, "profit_by_age_group_and_gender")

    spark.stop()

if __name__== '__main__':
    main()