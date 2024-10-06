import argparse
from typing import Optional

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, months_between, current_date, date_format, lit,  udf, to_date, row_number, desc, count, sum, month, round
from pyspark.sql.types import IntegerType, TimestampType, StringType, DecimalType, StructType, StructField, Row
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

_ADDRESS_SCHEMA = StructType([
    StructField('Street1', StringType(), True),
    StructField('Street2', StringType(), True),
    StructField('City', StringType(), False),
    StructField('Country', StringType(), False)
])

@udf(returnType=StringType())
def categorize_age_group(age: int) -> str:
    if age >= 60: return '60+'
    elif age >= 41: return '41-60'
    elif age >= 26: return '26-40'
    elif age >= 18: return '18-25'
    return '1-18'


@udf(returnType=_ADDRESS_SCHEMA)
def extract_address_info(address: str):
    street1: Optional[str] = ''
    street2: Optional[str] = ''
    city: Optional[str] = ''
    country: Optional[str] = ''

    splitted = address.split(',')

    if len(splitted) == 4:
        street1, street2, city, country = splitted[0],splitted[1],splitted[2],splitted[3]
    elif len(splitted) == 3:
        street1, city, country = splitted[0],splitted[1],splitted[2]

    return Row('Street1', 'Street2', 'City', 'Country')(street1, street2, city, country)


@udf(returnType=DecimalType(10, 2))
def update_total_price(qty: int, price: int, total_price: int) -> int:
    calculated_price: int = qty * price
    return calculated_price if total_price != calculated_price else total_price


def cleaning_users_df(df: DataFrame) -> DataFrame:
    # fix types of columns
    df = df.withColumn("id", col('id').cast(IntegerType())) \
        .withColumn('dob', col('dob').cast(TimestampType())) \
        .withColumn('age', round(months_between(current_date(), col('dob')) / lit(12), 0).cast(IntegerType())) \
        .withColumn('age_group', categorize_age_group(col('age'))) \
        .withColumn('parsed_address', extract_address_info(col('address'))) \
        .withColumn('street1', col('parsed_address').getField('Street1')) \
        .withColumn('street2', col('parsed_address').getField('Street2')) \
        .withColumn('city', col('parsed_address').getField('City')) \
        .withColumn('country', col('parsed_address').getField('Country')) \
        .drop('parsed_address')

    return df

def cleaning_orders_df(df: DataFrame) -> DataFrame:
    # fix types of columns
    df = df.withColumn("id", col('id').cast(IntegerType())) \
        .withColumn('order_date', col('order_date').cast(TimestampType())) \
        .withColumn('quantity', col('quantity').cast(IntegerType())) \
        .withColumn('unit_price', col('unit_price').cast(DecimalType(10, 2))) \
        .withColumn('total_price', col('total_price').cast(DecimalType(10, 2))) \
        .withColumn('total_price', update_total_price(col('quantity'), col('unit_price'), col('total_price'))) \
        .withColumn('parsed_address', extract_address_info(col('delivery_address'))) \
        .withColumn('street1', col('parsed_address').getField('Street1')) \
        .withColumn('street2', col('parsed_address').getField('Street2')) \
        .withColumn('city', col('parsed_address').getField('City')) \
        .withColumn('country', col('parsed_address').getField('Country')) \
        .drop('parsed_address') \
        .withColumn('date', to_date(col('order_date'))) \
        .withColumn('month', month(col('date'))) \

    return df

def cleaning_logs_df(df: DataFrame) -> DataFrame:
    # fix types of columns
    df = df.withColumn("id", col('id').cast(IntegerType())) \
        .withColumn('login_date', col('login_date').cast(TimestampType())) \
        .withColumn('logout_date', col('logout_date').cast(TimestampType()))

    return df

def prepare_address_df(users_df: DataFrame, orders_df: DataFrame) -> DataFrame:
    address_df: DataFrame = (users_df.alias('users')
                             .select(col('address'), col('street1'), col('street2'),col('city'), col('country'))
        .unionAll(orders_df.alias('orders')
                  .select(col('delivery_address'), col('street1'), col('street2'), col('city'), col('country')))
        .dropDuplicates(['address'])
        .withColumn('id', row_number().over(Window.orderBy('address')))
        .select(col('id'), col('address'), col('street1'), col('street2'), col('city'), col('country'))
    )

    return address_df

def prepare_genders_df(users_df: DataFrame) -> DataFrame:
    genders_tbl: DataFrame = users_df.select(col('gender')).distinct()\
            .withColumnRenamed('gender', 'name')\
            .withColumn('id', row_number().over(Window.orderBy('name')))\
            .select(['id', 'name'])
    return genders_tbl

def prepare_category_df(orders_df: DataFrame) -> DataFrame:
    category_tbl: DataFrame = orders_df.select(col('product_category')).distinct()\
            .withColumnRenamed('product_category', 'name')\
            .withColumn('id', row_number().over(Window.orderBy('name')))\
            .select(['id', 'name'])
    return category_tbl

def prepare_device_df(logs_df: DataFrame) -> DataFrame:
    device_tbl: DataFrame = logs_df.select(col('device')).distinct()\
            .withColumnRenamed('device', 'name')\
            .withColumn('id', row_number().over(Window.orderBy('name')))\
            .select(['id', 'name'])
    return device_tbl

def prepare_joined_users(users_df: DataFrame, gender_df: DataFrame, address_df: DataFrame) -> DataFrame:
    users_tbl_joined: DataFrame = users_df.alias('u') \
        .join(gender_df.alias('g'), on=col('u.gender') == col('g.name'), how='left') \
        .join(address_df.alias('add'), on=col('u.address') == col('add.address'), how='left') \
        .withColumn('dob', date_format(col('dob'), "yyyy-MM-dd hh:mm:ss")) \
        .select(
            col('u.id'),
            col('u.first_name'),
            col('u.last_name'),
            col('u.email'),
            col('add.id').alias('address_id'),
            col('g.id').alias('gender_id'),
            col('dob'),
            col('u.phone_number')
        )
    return users_tbl_joined

def prepare_joined_logs(logs_df: DataFrame, users_df: DataFrame, device_df: DataFrame) -> DataFrame:
    logs_tbl_joined: DataFrame = logs_df.alias("l") \
        .join(users_df.select(col('id'), col('email')).alias('u'), on=col('u.email') == col('l.user_email')) \
        .join(device_df.alias('d'), on=col('d.name') == col('l.device')) \
        .withColumn('login_date', date_format(col('login_date'), "yyyy-MM-dd hh:mm:ss")) \
        .withColumn('logout_date', date_format(col('logout_date'), "yyyy-MM-dd hh:mm:ss")) \
        .select(
            col('l.id'),
            col('login_date'),
            col('logout_date'),
            col('u.id').alias('user_id'),
            col('d.id').alias('device_id')
        )
    return logs_tbl_joined

def prepare_joined_product(orders_df: DataFrame, category_df: DataFrame) -> DataFrame:
    products_tbl: DataFrame = orders_df.alias('o') \
        .join(category_df.alias('c'), on=col('c.name') == col('o.product_category')) \
        .select(
            col('o.id'),
            col('o.product_name').alias('name'),
            col('c.id').alias('category_id'),
            col('o.product_brand').alias('brand'),
            col('o.product_description').alias('description'),
            col('o.unit_price').alias('price')
        ) \
        .withColumn('rnb', row_number().over(Window.partitionBy('name').orderBy(desc('id')))) \
        .where(col('rnb') == 1) \
        .drop('id', 'rnb') \
        .withColumn('id', row_number().over(Window.orderBy('name'))) \
        .select(
            col('id'),
            col('name'),
            col('category_id'),
            col('brand'),
            col('price'),
            col('description')
        )
    return products_tbl

def prepare_joined_orders(orders_df: DataFrame, users_df: DataFrame, address_df: DataFrame) -> DataFrame:
    orders_df_cleaned: DataFrame = orders_df.alias('o') \
        .groupBy('customer_email', 'date', 'delivery_address').agg( \
            count('customer_email').alias('order_items_amount'),
            sum('total_price').alias('order_total_price')
        ) \
        .withColumn('order_id', row_number().over(Window.orderBy('date', 'customer_email'))) \
        .join(users_df.alias('u'), on=col('u.email') == col('o.customer_email')) \
        .join(address_df.alias('a'), on=col('a.address') == col('o.delivery_address')) \
        .select(
            col('order_id').alias('id'),
            col('u.id').alias('customer_id'),
            col('u.email').alias('email'),
            col('o.date').alias('order_date'),
            col('order_total_price').alias('total_price'),
            col('u.id').alias('delivery_address_id')
        )
    return orders_df_cleaned

def prepare_joined_order_items(orders_df: DataFrame, orders_cleaned_df: DataFrame, product_df: DataFrame) -> DataFrame:
    order_items_tbl: DataFrame = orders_df.alias('oi') \
        .join(orders_cleaned_df.alias('o'), on=(col('oi.customer_email') == col('o.email')) & (col('oi.date') == col('o.order_date'))) \
        .join(product_df.alias('p'), on=col('p.name') == col('oi.product_name')) \
        .select(
            col('oi.id').alias('id'),
            col('o.id').alias('order_id'),
            col('p.id').alias('product_id'),
            col('oi.quantity'),
            col('oi.unit_price'),
            col('oi.total_price')
        )
    return order_items_tbl


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
    users_df: DataFrame = postgres_connection.load_data(spark, "public.users")
    logs_df: DataFrame = postgres_connection.load_data(spark, "public.logs")
    orders_df: DataFrame = postgres_connection.load_data(spark, "public.orders")
    users_df.cache()
    logs_df.cache()
    orders_df.cache()

    # cleaning data
    cleaned_users_df: DataFrame = cleaning_users_df(users_df)
    cleaned_logs_df: DataFrame = cleaning_logs_df(logs_df)
    cleaned_orders_df: DataFrame = cleaning_orders_df(orders_df)
    cleaned_users_df.cache()
    cleaned_logs_df.cache()
    cleaned_orders_df.cache()

    address_df: DataFrame = prepare_address_df(cleaned_users_df, cleaned_orders_df)
    gender_df: DataFrame = prepare_genders_df(cleaned_users_df)
    category_df: DataFrame = prepare_category_df(cleaned_orders_df)
    device_df: DataFrame = prepare_device_df(cleaned_logs_df)
    address_df.cache()
    gender_df.cache()
    category_df.cache()
    device_df.cache()

    users_full: DataFrame = prepare_joined_users(cleaned_users_df, gender_df, address_df)
    logs_full: DataFrame = prepare_joined_logs(cleaned_logs_df, cleaned_users_df, device_df)
    product_full: DataFrame = prepare_joined_product(cleaned_orders_df, category_df)
    orders_full: DataFrame = prepare_joined_orders(cleaned_orders_df, users_full, address_df)
    order_items_full: DataFrame = prepare_joined_order_items(cleaned_orders_df, orders_full, product_full)

    # save data
    postgres_connection.save_data_into_table(cleaned_users_df, "cleaned_users")
    postgres_connection.save_data_into_table(cleaned_logs_df, "cleaned_logs")
    postgres_connection.save_data_into_table(cleaned_orders_df, "cleaned_orders")

    postgres_connection.save_data_into_table(address_df, "address")
    postgres_connection.save_data_into_table(gender_df, "gender")
    postgres_connection.save_data_into_table(category_df, "category")
    postgres_connection.save_data_into_table(device_df, "device")

    postgres_connection.save_data_into_table(users_full, "users_full")
    postgres_connection.save_data_into_table(logs_full, "logs_full")
    postgres_connection.save_data_into_table(product_full, "product_full")
    postgres_connection.save_data_into_table(orders_full, "orders_full")
    postgres_connection.save_data_into_table(order_items_full, "order_items_full")

    spark.stop()

if __name__== '__main__':
    main()