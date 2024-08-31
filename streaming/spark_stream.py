import sys
import logging

from cassandra.cluster import Cluster
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType

import findspark
findspark.init()
findspark.find()


log_file = 'spark_stream_log.log'
logging.basicConfig(filename=log_file, level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

def create_keyspace(session):
    session.execute("""
            CREATE KEYSPACE IF NOT EXISTS spark_streams
            WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'};
        """)

    print("Keyspace created successfully!")
    logging.info("Keyspace created successfully!")


def create_table(session):
    session.execute("""
        CREATE TABLE IF NOT EXISTS spark_streams.created_users (
            id UUID PRIMARY KEY,
            first_name TEXT,
            last_name TEXT,
            gender TEXT,
            address TEXT,
            post_code TEXT,
            email TEXT,
            username TEXT,
            registered_date TEXT,
            phone TEXT,
            picture TEXT);
        """)

    print("Table created successfully!")
    logging.info("Table created successfully!")

def insert_data(session, **kwargs):
    logging.info("inserting data...")

    user_id = kwargs.get('id')
    first_name = kwargs.get('first_name')
    last_name = kwargs.get('last_name')
    gender = kwargs.get('gender')
    address = kwargs.get('address')
    postcode = kwargs.get('post_code')
    email = kwargs.get('email')
    username = kwargs.get('username')
    dob = kwargs.get('dob')
    registered_date = kwargs.get('registered_date')
    phone = kwargs.get('phone')
    picture = kwargs.get('picture')

    try:
        session.execute("""
                INSERT INTO spark_streams.created_users(id, first_name, last_name, gender, address, 
                    post_code, email, username, dob, registered_date, phone, picture)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (user_id, first_name, last_name, gender, address,
                  postcode, email, username, dob, registered_date, phone, picture))
        logging.info(f"Data inserted for {first_name} {last_name}")

    except Exception as e:
        logging.error(f'could not insert data due to {e}')

def create_spark_connection():
    conn = None

    try:
        # https://mvnrepository.com/artifact/com.datastax.spark/spark-cassandra-connector
        # https://mvnrepository.com/artifact/org.apache.spark/spark-sql-kafka-0-10
        conn = SparkSession.builder\
            .appName('SparkDataStreamin') \
            .config('spark.jars.packages', "com.datastax.spark:spark-cassandra-connector_2.13:3.5.1,"
                                           "org.apache.spark:spark-sql-kafka-0-10_2.13:3.5.2") \
            .config('spark.cassandra.connection.host', 'localhost') \
            .getOrCreate()
        conn.sparkContext.setLogLevel('ERROR')
        logging.info('Spark connection created successfully.')
    except Exception as e:
        logging.error(f"Couldn't create the spark session due to exception {e}")

    return conn

def create_cassandra_connection():
    session = None

    try:
        # connection to cassandra cluster
        cluster = Cluster(['localhost'])

        # generate session
        session = cluster.connect()
    except Exception as e:
        logging.error(f"Couldn't connect to the cassandra cluster due to exception {e}")

    return session

def connect_to_kafka(spark_conn):
    spark_df = None
    try:
        spark_df = spark_conn.readStream \
            .format('kafka') \
            .option('kafka.bootstrap.servers', 'localhost:9092') \
            .option('subscribe', 'users_created') \
            .option('startingOffsets', 'earliest') \
            .load()
        logging.info("kafka dataframe created successfully")
    except Exception as e:
        logging.warning(f"kafka dataframe could not be created because: {e}")

    return spark_df

def create_selection_df_from_kafka(spark_df):
    schema = StructType([
        StructField("id", StringType(), False),
        StructField("first_name", StringType(), False),
        StructField("last_name", StringType(), False),
        StructField("gender", StringType(), False),
        StructField("address", StringType(), False),
        StructField("post_code", StringType(), False),
        StructField("email", StringType(), False),
        StructField("username", StringType(), False),
        StructField("registered_date", StringType(), False),
        StructField("phone", StringType(), False),
        StructField("picture", StringType(), False)
    ])

    sel = spark_df.selectExpr("CAST(value AS STRING)") \
        .select(from_json(col('value'), schema).alias('data')).select("data.*")
    print(sel)

    return sel

if __name__ == '__main__':
    spark_conn = create_spark_connection()
    if spark_conn is None:
        sys.exit(1)

    session = create_cassandra_connection()
    if session is None:
        sys.exit(1)

    create_keyspace(session)
    create_table(session)

    df = connect_to_kafka(spark_conn)
    if df is None:
        sys.exit(1)

    # selection_df = create_selection_df_from_kafka(df)
    #
    # logging.info("Streaming is being started...")
    #
    # streaming_query = (selection_df.writeStream.format("org.apache.spark.sql.cassandra")
    #                    .option('checkpointLocation', '/tmp/checkpoint')
    #                    .option('keyspace', 'spark_streams')
    #                    .option('table', 'created_users')
    #                    .start())
    #
    # streaming_query.awaitTermination()


