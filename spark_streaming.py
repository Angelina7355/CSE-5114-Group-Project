from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr, from_json, to_timestamp, from_unixtime, window
from pyspark.sql.types import *
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization

# ---------- For logging in Snowflake ----------
def get_private_key_string(key_path, password=None):
    """Reads a PEM private key and returns the string format required by PySpark."""
    with open(key_path, "rb") as key_file:
        p_key = serialization.load_pem_private_key(
            key_file.read(),
            password=password.encode() if password else None,
            backend=default_backend()
        )

    pkb = p_key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption()
    )

    # Spark requires the raw key string without headers, footers, or newlines
    pkb_str = pkb.decode("utf-8")
    pkb_str = pkb_str.replace("-----BEGIN PRIVATE KEY-----", "")
    pkb_str = pkb_str.replace("-----END PRIVATE KEY-----", "")
    pkb_str = pkb_str.replace("\n", "")
    return pkb_str

#----------------------------------------------------------#
#                     Spark Streaming                      #
#----------------------------------------------------------#
def create_spark(app_name="WeatherTrafficStreaming"):
    spark = SparkSession.builder \
        .appName(app_name) \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,net.snowflake:spark-snowflake_2.12:2.15.0-spark_3.4") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    return spark

# --- Define Schemas ---
weather_schema = StructType([
    StructField("source", StringType(), True),
    StructField("timestamp", LongType(), True),
    StructField("temp", DoubleType(), True),
    StructField("humidity", IntegerType(), True),
    StructField("wind_speed", DoubleType(), True),
    StructField("weather", StringType(), True),
    StructField("lat", DoubleType(), True),
    StructField("lon", DoubleType(), True),
    StructField("ingestion_time", StringType(), True)
])

# Matches the flat structure returned by fetching_incidents()
traffic_schema = StructType([
    StructField("source", StringType(), True),
    StructField("id", StringType(), True),
    StructField("type", StringType(), True),
    StructField("severity", StringType(), True),
    StructField("description", StringType(), True),
    StructField("start_time", StringType(), True),
    StructField("lat", DoubleType(), True),
    StructField("lon", DoubleType(), True),
    StructField("ingestion_time", StringType(), True)
])

#----------------------------------------------------------#
#                         Main Method                      #
#----------------------------------------------------------#

if __name__ == "__main__":
        
    # --- Define Spark Streams and Fetch JSON from Kafka ---
    spark = create_spark()

    weather_stream = spark.readStream.format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "weather_topic") \
        .option("startingOffsets", "latest") \
        .load()

    traffic_stream = spark.readStream.format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "traffic_topic") \
        .option("startingOffsets", "latest") \
        .load()

    # --- Structure stream data ---
    # Parses JSON from Kafka using the flat schemas and aliases columns for the join
    weather_stream = weather_stream.select(from_json(col("value").cast("string"), weather_schema).alias("data")) \
        .select(
            col("data.lat").alias("w_lat"),
            col("data.lon").alias("w_lon"),
            from_unixtime(col("data.timestamp")).cast("timestamp").alias("w_timestamp"),
            col("data.temp").alias("temp"),
            col("data.humidity").alias("humidity"),
            col("data.wind_speed").alias("wind_speed"),
            col("data.weather").alias("weather_desc")
        )
    weather_stream = weather_stream.withWatermark("w_timestamp", "10 minutes")
    weather_stream = weather_stream.withColumn("w_minute", (col("w_timestamp").cast("long") / 60).cast("long"))

    traffic_stream = traffic_stream.select(from_json(col("value").cast("string"), traffic_schema).alias("data")) \
        .select(
            col("data.id").alias("t_id"),
            col("data.type").alias("type"),
            col("data.severity").alias("severity"),
            col("data.description").alias("description"),
            to_timestamp(col("data.start_time")).alias("t_start"),
            col("data.lat").alias("t_lat"),
            col("data.lon").alias("t_lon")
        )
    traffic_stream = traffic_stream.dropDuplicates(["t_id"])
    traffic_stream = traffic_stream.withWatermark("t_start", "10 minutes")
    traffic_stream = traffic_stream.withColumn("t_minute", (col("t_start").cast("long") / 60).cast("long"))

    # --- Join streams by time (minute) ---
    joined_stream = traffic_stream.join(
        weather_stream,
        expr("t_minute = w_minute"),
        how="leftOuter"
    )
    sliding_window = joined_stream.groupBy(window("t_start", "30 minutes", "10 minutes"), col("weather_desc")).count()

    pkb_string = get_private_key_string("rsa_key.p8")
    # --- Write to Snowflake ---
    sf_options = {
      "sfURL": "sfedu02-unb02139.snowflakecomputing.com",
      "sfUser": "JELLYFISH",
      "sfDatabase": "JELLYFISH_DB",
      "sfSchema": "JELLYFISH_SCHEMA",
      "sfWarehouse": "JELLYFISH_WH",
      "pem_private_key": pkb_string
    }

    def write_to_snowflake(batch_df, epoch_id):
        batch_df.write \
            .format("net.snowflake.spark.snowflake") \
            .options(**sf_options) \
            .option("dbtable", "weather_traffic_comb") \
            .mode("append") \
            .save()

    query = sliding_window.writeStream \
        .foreachBatch(write_to_snowflake) \
        .outputMode("append") \
        .option("checkpointLocation", "/tmp/spark_checkpoint") \
        .start()

    query.awaitTermination()





