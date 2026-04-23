# import logging
# from cassandra.cluster import Cluster
# from cassandra.auth import PlainTextAuthProvider
# from pyspark.sql import SparkSession
# from pyspark.sql.functions import from_json, col
# from pyspark.sql.types import StructType, StructField, StringType
# from datetime import datetime

# def create_keyspace(session):
#         session.execute("""
#         CREATE KEYSPACE IF NOT EXISTS spark_streams
#         WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'};
#     """)
#         print("Keyspace created successfully!")

    
# def create_table(session):
#         session.execute("""
#         CREATE TABLE IF NOT EXISTS spark_streams.created_users (
#             id TEXT PRIMARY KEY,
#             first_name TEXT,
#             last_name TEXT,
#             gender TEXT,
#             address TEXT,
#             post_code TEXT,
#             email TEXT,
#             username TEXT,
#             registered_date TEXT,
#             phone TEXT,
#             picture TEXT
#         );
#     """)
#         print("Table created successfully!")
    
# def insert_data(session, **kwargs):
#     print("inserting data...")
    
#     id = kwargs.get('id')
#     first_name = kwargs.get('first_name')
#     last_name = kwargs.get('last_name')
#     gender = kwargs.get('gender')
#     address = kwargs.get('address')
#     email = kwargs.get('email')
#     user_name = kwargs.get('user_name')
#     dob = kwargs.get('dob')
#     registered_date = kwargs.get('registered_name')
#     phone = kwargs.get('phone')
#     picture = kwargs.get('picture')
    
#     try:
#         session.excute(""" 
#             INSERT INTO spark_streams.created_users(id, first_name, last_name, gender, address,
#             postcode, email, username, dob, registered_data, phone, picture)
#             VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)           
#         """, (id, first_name, last_name, gender, address,
#                              email, user_name, dob, registered_date, phone, picture))
#         logging.info("Data inserted for {first_name} {last_name}")
        
#     except Exception as e:
#         logging.error(f'could not insert data due to {e}')    
    
# def create_spark_connection():
#     s_conn = None
#     try:
#         s_conn = SparkSession.builder \
#             .appName('SparkDataStreaming') \
#             .config('spark.jars.packages',"com.datastax.spark:spark-cassandra-connector_2.12:3.3.0,"
#              "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0") \
#             .config('spark.cassandra.connection.host', 'cassandra') \
#             .getOrCreate()

#         s_conn.sparkContext.setLogLevel("ERROR")
#         logging.info("Spark connection created successfully!")

#     except Exception as e:
#         logging.error(f"Couldn't create the spark due to exception {e}")
        
#     return s_conn

# def connect_to_kafka(spark_conn):
#     spark_df = None
#     try:
#         spark_df = spark_conn.readStream \
#             .format('kafka') \
#             .option('kafka.bootstrap.servers', 'broker:9092')  \
#             .option('subscribe', 'users_created')  \
#             .option('startingOffsets', 'earliest') \
#             .load()
#         logging.info("kafka dataframe created successfully")
#     except Exception as e:
#         logging.warning(f"kafka dataframe could not be created because: {e}")         
    
#     return spark_df
         
# def create_cassandra_connection(): 

#     # create cassandra connection
#     try:
#         cluster = Cluster(['cassandra'])
        
#         cas_session = cluster.connect()
        
#         return cas_session
#     except Exception as e:
#         logging.error("Could not create cassandra connection due to {e}")    
#         return None

# def create_selection_df_from_kafka(spark_df):
#     schema = StructType([
#         StructField("id", StringType(), False),
#         StructField("first_name", StringType(), False),
#         StructField("last_name", StringType(), False),
#         StructField("gender", StringType(), False),
#         StructField("address", StringType(), False),
#         StructField("post_code", StringType(), False),
#         StructField("email", StringType(), False),
#         StructField("username", StringType(), False),
#         StructField("registered_date", StringType(), False),
#         StructField("phone", StringType(), False),
#         StructField("picture", StringType(), False)
#     ])

#     df = spark_df.selectExpr("CAST(value AS STRING)") \
#         .select(from_json(col("value"), schema).alias("data")) \
#         .select("data.*")

#     return df

# if __name__ == "__main__":
#     #create spark connection
#     spark_conn = create_spark_connection()  
    
#     if spark_conn is not None: 
#         #connect to kafka with spark connection
#         spark_df = connect_to_kafka(spark_conn) 
#         selection_df =  create_selection_df_from_kafka(spark_df)
#         session = create_cassandra_connection()
        
#         if session is not None:
#             create_keyspace(session) 
#             create_table(session) 
#             # insert_data(session)   
            
#             logging.info("Streaming is being started...")
            
#             streaming_query = (selection_df.writeStream
#                             .format("org.apache.spark.sql.cassandra")
#                             .option('checkpointLocation', '/tmp/checkpoint')
#                             .option('keyspace', 'spark_streams') 
#                             .option('table','created_users')
#                             .start()) 
#             streaming_query.awaitTermination()

##################################################################spark for user_created and cassandra
# from pyspark.sql import SparkSession
# from pyspark.sql.functions import from_json, col, when
# from pyspark.sql.types import StructType, StructField, StringType, FloatType

# # ==============================
# # 1. Create Spark Session
# # ==============================
# spark = SparkSession.builder \
#     .appName("FireRiskStreaming") \
#     .config("spark.sql.shuffle.partitions", "2") \
#     .getOrCreate()

# spark.sparkContext.setLogLevel("WARN")

# # ==============================
# # 2. Define Schema
# # ==============================
# schema = StructType([
#     StructField("sensor_id", StringType(), True),
#     StructField("temperature", FloatType(), True),
#     StructField("humidity", FloatType(), True),
#     StructField("wind_speed", FloatType(), True),
#     StructField("vegetation_type", StringType(), True),
#     StructField("soil_moisture", FloatType(), True),
#     StructField("timestamp", StringType(), True),
# ])

# # ==============================
# # 3. Read from Kafka
# # ==============================
# df = spark.readStream \
#     .format("kafka") \
#     .option("kafka.bootstrap.servers", "broker:9092") \
#     .option("subscribe", "fire_sensors") \
#     .option("startingOffsets", "latest") \
#     .load()

# # ==============================
# # 4. Parse JSON
# # ==============================
# df_parsed = df.selectExpr("CAST(value AS STRING) as json_str") \
#     .select(from_json(col("json_str"), schema).alias("data")) \
#     .select("data.*")

# # ==============================
# # 5. Remove invalid records
# # ==============================
# df_clean = df_parsed.filter(
#     col("temperature").isNotNull() &
#     col("humidity").isNotNull() &
#     col("wind_speed").isNotNull() &
#     col("soil_moisture").isNotNull()
# )

# # ==============================
# # 6. Compute Fire Risk
# # ==============================
# risk_df = df_clean.withColumn(
#     "fire_risk_score",
#     when(col("temperature") > 40, 2).otherwise(0) +
#     when(col("humidity") < 20, 2).otherwise(0) +
#     when(col("wind_speed") > 20, 1).otherwise(0) +
#     when(col("soil_moisture") < 20, 1).otherwise(0)
# )

# risk_df = risk_df.withColumn(
#     "status",
#     when(col("fire_risk_score") >= 4, "🔥 HIGH RISK")
#     .when(col("fire_risk_score") >= 2, "⚠️ MEDIUM RISK")
#     .otherwise("✅ SAFE")
# )

# # ==============================
# # 7. Output to Console
# # ==============================
# query = risk_df.writeStream \
#     .outputMode("append") \
#     .format("console") \
#     .option("truncate", False) \
#     .start()

# # ==============================
# # 8. Run Stream
# # ==============================
# query.awaitTermination()

######################################spark for working with fire risk kafka and spark without ml



# from pyspark.sql import SparkSession
# from pyspark.sql.functions import from_json, col, when, udf
# from pyspark.sql.types import StructType, StructField, StringType, FloatType, IntegerType
# import joblib

# # ==============================
# # 1. Create Spark Session (FIXED)
# # ==============================
# spark = SparkSession.builder \
#     .appName("FireRiskStreamingML") \
#     .getOrCreate()

# spark.sparkContext.setLogLevel("WARN")

# # ==============================
# # 2. Load ML Model
# # ==============================
# model = joblib.load("/opt/spark/work-dir/fire_model_fix.pkl")

# # ==============================
# # 3. Prediction Function
# # ==============================
# def predict_fire(temp, humidity, wind, soil):
#     try:
#         features = [[float(temp), float(humidity), float(wind), float(soil)]]
#         return int(model.predict(features)[0])
#     except:
#         return 0

# predict_udf = udf(predict_fire, IntegerType())

# # ==============================
# # 4. Schema
# # ==============================
# schema = StructType([
#     StructField("sensor_id", StringType(), True),
#     StructField("temperature", FloatType(), True),
#     StructField("humidity", FloatType(), True),
#     StructField("wind_speed", FloatType(), True),
#     StructField("vegetation_type", StringType(), True),
#     StructField("soil_moisture", FloatType(), True),
#     StructField("timestamp", StringType(), True),
# ])

# # ==============================
# # 5. Read from Kafka
# # ==============================
# df = spark.readStream \
#     .format("kafka") \
#     .option("kafka.bootstrap.servers", "broker:9092") \
#     .option("subscribe", "fire_sensors") \
#     .option("startingOffsets", "latest") \
#     .load()

# # DEBUG (VERY IMPORTANT)
# df.printSchema()

# # ==============================
# # 6. Parse JSON
# # ==============================
# df_parsed = df.selectExpr("CAST(value AS STRING) as json_str") \
#     .select(from_json(col("json_str"), schema).alias("data")) \
#     .select("data.*")

# # ==============================
# # 7. Clean Data
# # ==============================
# df_clean = df_parsed.filter(
#     col("temperature").isNotNull() &
#     col("humidity").isNotNull() &
#     col("wind_speed").isNotNull() &
#     col("soil_moisture").isNotNull()
# )

# # ==============================
# # 8. Apply ML Model
# # ==============================
# ml_df = df_clean.withColumn(
#     "prediction",
#     predict_udf(
#         col("temperature"),
#         col("humidity"),
#         col("wind_speed"),
#         col("soil_moisture")
#     )
# )

# # ==============================
# # 9. Add Status
# # ==============================
# ml_df = ml_df.withColumn(
#     "status",
#     when(col("prediction") == 1, "🔥 HIGH RISK")
#     .otherwise("✅ SAFE")
# )

# # ==============================
# # 10. OUTPUT (FIXED)
# # ==============================
# query = ml_df.writeStream \
#     .outputMode("append") \
#     .format("console") \
#     .option("truncate", False) \
#     .trigger(processingTime="5 seconds") \
#     .start()

# # ==============================
# # 11. RUN
# # ==============================
# query.awaitTermination()

################################################################ workin spark with model traning #############################


# from pyspark.sql import SparkSession
# from pyspark.sql.functions import from_json, col, when, udf
# from pyspark.sql.types import StructType, StructField, StringType, FloatType, IntegerType
# import joblib

# # ==============================
# # 1. Create Spark Session (FIXED)
# # ==============================
# spark = SparkSession.builder \
#     .appName("FireRiskStreamingML") \
#     .config("spark.jars", "/opt/spark/jars/*") \
#     .getOrCreate()

# spark.sparkContext.setLogLevel("WARN")

# # ==============================
# # 2. Load ML Model
# # ==============================
# model = joblib.load("/opt/spark/work-dir/fire_model_fix.pkl")

# # ==============================
# # 3. Prediction Function
# # ==============================
# def predict_fire(temp, humidity, wind, soil):
#     try:
#         features = [[float(temp), float(humidity), float(wind), float(soil)]]
#         return int(model.predict(features)[0])
#     except:
#         return 0

# predict_udf = udf(predict_fire, IntegerType())

# # ==============================
# # 4. Schema
# # ==============================
# schema = StructType([
#     StructField("sensor_id", StringType(), True),
#     StructField("temperature", FloatType(), True),
#     StructField("humidity", FloatType(), True),
#     StructField("wind_speed", FloatType(), True),
#     StructField("vegetation_type", StringType(), True),
#     StructField("soil_moisture", FloatType(), True),
#     StructField("timestamp", StringType(), True),
# ])

# # ==============================
# # 5. Read from Kafka
# # ==============================
# df = spark.readStream \
#     .format("kafka") \
#     .option("kafka.bootstrap.servers", "broker:9092") \
#     .option("subscribe", "fire_sensors") \
#     .option("startingOffsets", "latest") \
#     .load()

# # DEBUG (VERY IMPORTANT)
# df.printSchema()

# # ==============================
# # 6. Parse JSON
# # ==============================
# df_parsed = df.selectExpr("CAST(value AS STRING) as json_str") \
#     .select(from_json(col("json_str"), schema).alias("data")) \
#     .select("data.*")

# # ==============================
# # 7. Clean Data
# # ==============================

# from pyspark.sql.functions import lit, when, col

# df_clean = df_parsed.filter(
#     col("temperature").isNotNull() &
#     col("humidity").isNotNull() &
#     col("wind_speed").isNotNull() &
#     col("soil_moisture").isNotNull()
# ).withColumn(
#     "vegetation_type",
#     when(col("vegetation_type").isNull(), lit("unknown"))
#     .otherwise(col("vegetation_type"))
# )

# ml_df = df_clean.select(
#     "sensor_id",
#     "temperature",
#     "humidity",
#     "wind_speed",
#     "soil_moisture",
#     "vegetation_type",   # ✅ ADD THIS
#     "timestamp"
# ).withColumn(
#     "prediction",
#     predict_udf(
#         col("temperature"),
#         col("humidity"),
#         col("wind_speed"),
#         col("soil_moisture")
#     )
# )

# # ==============================
# # 9. Add Status
# # ==============================
# ml_df = ml_df.withColumn(
#     "status",
#     when(col("prediction") == 1, "🔥 HIGH RISK")
#     .otherwise("✅ SAFE")
# )
# def write_raw_to_postgres(batch_df, batch_id):
#     batch_df.write \
#         .format("jdbc") \
#         .option("url", "jdbc:postgresql://postgres:5432/fire_db") \
#         .option("dbtable", "fire_sensors") \
#         .option("user", "user") \
#         .option("password", "password") \
#         .option("driver", "org.postgresql.Driver") \
#         .mode("append") \
#         .save()


# def write_pred_to_postgres(batch_df, batch_id):

#     batch_df = batch_df.select(
#         "sensor_id",
#         "temperature",
#         "humidity",
#         "wind_speed",
#         "soil_moisture",
#         "vegetation_type",
#         "prediction",
#         "status",
#         "timestamp"
#     )

#     batch_df.write \
#         .format("jdbc") \
#         .option("url", "jdbc:postgresql://postgres:5432/fire_db") \
#         .option("dbtable", "fire_predictions") \
#         .option("user", "user") \
#         .option("password", "password") \
#         .option("driver", "org.postgresql.Driver") \
#         .mode("append") \
#         .save()
# # ==============================
# # 10. OUTPUT (FIXED)
# # ==============================
# # ==============================
# # 10. WRITE STREAMS
# # ==============================

# # 🔹 Save raw sensor data
# raw_query = df_clean.writeStream \
#     .foreachBatch(write_raw_to_postgres) \
#     .outputMode("append") \
#     .start()

# # 🔹 Save predictions
# pred_query = ml_df.writeStream \
#     .foreachBatch(write_pred_to_postgres) \
#     .outputMode("append") \
#     .start()

# # 🔹 Console output (keep for debugging)
# console_query = ml_df.writeStream \
#     .outputMode("append") \
#     .format("console") \
#     .option("truncate", False) \
#     .trigger(processingTime="5 seconds") \
#     .start()

# # ==============================
# # 11. RUN
# # ==============================
# spark.streams.awaitAnyTermination()
#########################################################working code for the postgresql

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, when, udf
from pyspark.sql.types import StructType, StructField, StringType, FloatType, IntegerType
import joblib

# ==============================
# 1. Create Spark Session (FIXED)
# ==============================
spark = SparkSession.builder \
    .appName("FireRiskStreamingML") \
    .config("spark.jars", "/opt/spark/jars/*") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

import requests

BOT_TOKEN = "8680424283:AAFeF4pyaw2ZCvTvwTKpMWKOm9TSLJ7YWjQ"
CHAT_ID = "6242105556"

def send_telegram_alert(message):
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    payload = {
        "chat_id": CHAT_ID,
        "text": message
    }
    requests.post(url, data=payload)
# ==============================
# 2. Load ML Model
# ==============================
model = joblib.load("/opt/spark/work-dir/fire_model_fix.pkl")

# ==============================
# 3. Prediction Function
# ==============================
def predict_fire(temp, humidity, wind, soil):
    try:
        features = [[float(temp), float(humidity), float(wind), float(soil)]]
        return int(model.predict(features)[0])
    except:
        return 0

predict_udf = udf(predict_fire, IntegerType())

# ==============================
# 4. Schema
# ==============================
schema = StructType([
    StructField("sensor_id", StringType(), True),
    StructField("temperature", FloatType(), True),
    StructField("humidity", FloatType(), True),
    StructField("wind_speed", FloatType(), True),
    StructField("vegetation_type", StringType(), True),
    StructField("soil_moisture", FloatType(), True),
    StructField("timestamp", StringType(), True),
])

# ==============================
# 5. Read from Kafka
# ==============================
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "broker:9092") \
    .option("subscribe", "fire_sensors") \
    .option("startingOffsets", "latest") \
    .load()

# DEBUG (VERY IMPORTANT)
df.printSchema()

# ==============================
# 6. Parse JSON
# ==============================
df_parsed = df.selectExpr("CAST(value AS STRING) as json_str") \
    .select(from_json(col("json_str"), schema).alias("data")) \
    .select("data.*")

# ==============================
# 7. Clean Data
# ==============================

from pyspark.sql.functions import lit, when, col

df_clean = df_parsed.filter(
    col("temperature").isNotNull() &
    col("humidity").isNotNull() &
    col("wind_speed").isNotNull() &
    col("soil_moisture").isNotNull()
).withColumn(
    "vegetation_type",
    when(col("vegetation_type").isNull(), lit("unknown"))
    .otherwise(col("vegetation_type"))
)

ml_df = df_clean.select(
    "sensor_id",
    "temperature",
    "humidity",
    "wind_speed",
    "soil_moisture",
    "vegetation_type",   # ✅ ADD THIS
    "timestamp"
).withColumn(
    "prediction",
    predict_udf(
        col("temperature"),
        col("humidity"),
        col("wind_speed"),
        col("soil_moisture")
    )
)

# ==============================
# 9. Add Status
# ==============================
ml_df = ml_df.withColumn(
    "status",
    when(col("prediction") == 1, "🔥 HIGH RISK")
    .otherwise("✅ SAFE")
)
def write_raw_to_postgres(batch_df, batch_id):
    batch_df.write \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://postgres:5432/fire_db") \
        .option("dbtable", "fire_sensors") \
        .option("user", "user") \
        .option("password", "password") \
        .option("driver", "org.postgresql.Driver") \
        .mode("append") \
        .save()
        
last_status = {}

def write_pred_to_postgres(batch_df, batch_id):

    rows = batch_df.collect()

    for row in rows:
        sensor_id = row["sensor_id"]
        prediction = row["prediction"]

        previous = last_status.get(sensor_id)

        # Send alert only if NEW high risk
        if prediction == 1 and previous != 1:
            message = f"""
🔥 FIRE ALERT 🔥
Sensor: {sensor_id}
Temperature: {row['temperature']}
Risk: HIGH
Time: {row['timestamp']}
"""
            send_telegram_alert(message)

        # Update state
        last_status[sensor_id] = prediction

    # Write to DB
    batch_df.write \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://postgres:5432/fire_db") \
        .option("dbtable", "fire_predictions") \
        .option("user", "user") \
        .option("password", "password") \
        .option("driver", "org.postgresql.Driver") \
        .mode("append") \
        .save()        


# def write_pred_to_postgres(batch_df, batch_id):

#     # Get HIGH RISK rows
#     high_risk_df = batch_df.filter("prediction = 1")

#     if high_risk_df.count() > 0:
#         rows = high_risk_df.collect()

#         for row in rows:
#             message = f"""
# 🔥 FIRE ALERT 🔥
# Sensor: {row['sensor_id']}
# Temperature: {row['temperature']}
# Humidity: {row['humidity']}
# Risk: HIGH
# Time: {row['timestamp']}
# """
#             send_telegram_alert(message)

#     # Write to PostgreSQL
#     batch_df.write \
#         .format("jdbc") \
#         .option("url", "jdbc:postgresql://postgres:5432/fire_db") \
#         .option("dbtable", "fire_predictions") \
#         .option("user", "user") \
#         .option("password", "password") \
#         .option("driver", "org.postgresql.Driver") \
#         .mode("append") \
#         .save()
# # ==============================
# 10. OUTPUT (FIXED)
# ==============================
# ==============================
# 10. WRITE STREAMS
# ==============================

# 🔹 Save raw sensor data
raw_query = df_clean.writeStream \
    .foreachBatch(write_raw_to_postgres) \
    .outputMode("append") \
    .start()

# 🔹 Save predictions
pred_query = ml_df.writeStream \
    .foreachBatch(write_pred_to_postgres) \
    .outputMode("append") \
    .start()

# 🔹 Console output (keep for debugging)
console_query = ml_df.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", False) \
    .trigger(processingTime="5 seconds") \
    .start()

# ==============================
# 11. RUN
# ==============================
spark.streams.awaitAnyTermination()