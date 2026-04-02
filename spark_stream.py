import logging
from cassandra.cluster import Cluster
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType

logging.basicConfig(level=logging.INFO)


# ---------------------------
# Cassandra Setup
# ---------------------------

def create_keyspace(session):
    session.execute("""
        CREATE KEYSPACE IF NOT EXISTS spark_streams
        WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'};
    """)
    print("Keyspace created successfully!")


def create_table(session):
    session.execute("""
        CREATE TABLE IF NOT EXISTS spark_streams.created_users (
            id TEXT PRIMARY KEY,
            first_name TEXT,
            last_name TEXT,
            gender TEXT,
            address TEXT,
            post_code TEXT,
            email TEXT,
            username TEXT,
            registered_date TEXT,
            phone TEXT,
            picture TEXT
        );
    """)
    print("Table created successfully!")


# ---------------------------
# Spark Setup
# ---------------------------

def create_spark_connection():
    try:
        spark = SparkSession.builder \
            .appName("SparkDataStreaming") \
            .config("spark.jars.packages",
                    "com.datastax.spark:spark-cassandra-connector_2.12:3.3.0,"
                    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0") \
            .config("spark.cassandra.connection.host", "cassandra") \
            .getOrCreate()

        spark.sparkContext.setLogLevel("ERROR")
        print("Spark connection created successfully!")
        return spark

    except Exception as e:
        logging.error(f"Spark connection error: {e}")
        return None


# ---------------------------
# Kafka Connection
# ---------------------------

def connect_to_kafka(spark):
    try:
        df = spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "broker:9092") \
            .option("subscribe", "users_created") \
            .option("startingOffsets", "earliest") \
            .load()

        print("Kafka connection successful!")
        return df

    except Exception as e:
        logging.error(f"Kafka connection error: {e}")
        return None


# ---------------------------
# Cassandra Connection
# ---------------------------

def create_cassandra_connection():
    try:
        cluster = Cluster(["cassandra"])  # ✅ FIXED
        session = cluster.connect()
        return session
    except Exception as e:
        logging.error(f"Cassandra connection error: {e}")
        return None


# ---------------------------
# Transform Kafka Data
# ---------------------------

def create_selection_df_from_kafka(kafka_df):
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

    df = kafka_df.selectExpr("CAST(value AS STRING)") \
        .select(from_json(col("value"), schema).alias("data")) \
        .select("data.*")

    return df


# ---------------------------
# Main
# ---------------------------

if __name__ == "__main__":

    spark = create_spark_connection()

    if spark:
        kafka_df = connect_to_kafka(spark)
        selection_df = create_selection_df_from_kafka(kafka_df)

        session = create_cassandra_connection()

        if session:
            create_keyspace(session)
            create_table(session)

            print("Starting streaming...")

            query = selection_df.writeStream \
                .format("org.apache.spark.sql.cassandra") \
                .option("checkpointLocation", "/tmp/checkpoint") \
                .option("keyspace", "spark_streams") \
                .option("table", "created_users") \
                .start()

            query.awaitTermination()