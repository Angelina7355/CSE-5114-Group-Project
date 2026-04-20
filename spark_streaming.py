#----------------------------------------------------------#
#                    Imports & Setup                       #
#----------------------------------------------------------#
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from datetime import datetime, UTC

from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization

import os
import json
import redis

PROGRAM_START_TIME = datetime.now(UTC)


#----------------------------------------------------------#
#           Snowflake Private Key Handling                 #
#----------------------------------------------------------#

def get_private_key_string(key_path, password=None):
    """
    Reads PEM private key and formats it for Spark Snowflake connector.
    """
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

    pkb_str = pkb.decode("utf-8")
    pkb_str = pkb_str.replace("-----BEGIN PRIVATE KEY-----", "")
    pkb_str = pkb_str.replace("-----END PRIVATE KEY-----", "")
    pkb_str = pkb_str.replace("\n", "")

    return pkb_str


#----------------------------------------------------------#
#                Spark Session Creation                    #
#----------------------------------------------------------#

def create_spark():
    spark = SparkSession.builder \
        .appName("WeatherTrafficStreaming") \
        .config(
            "spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,"
            "net.snowflake:spark-snowflake_2.12:2.15.0-spark_3.4"
        ) \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    spark.conf.set("spark.sql.session.timeZone", "UTC")

    return spark


#----------------------------------------------------------#
#                    Define Schemas                        #
#----------------------------------------------------------#

weather_schema = StructType([
    StructField("source", StringType()),
    StructField("timestamp", LongType()),
    StructField("temp", DoubleType()),
    StructField("humidity", IntegerType()),
    StructField("wind_speed", DoubleType()),
    StructField("weather", StringType()),
    StructField("lat", DoubleType()),
    StructField("lon", DoubleType()),
    StructField("ingestion_time", StringType())
])

traffic_schema = StructType([
    StructField("source", StringType()),
    StructField("id", StringType()),
    StructField("type", StringType()),
    StructField("severity", StringType()),
    StructField("description", StringType()),
    StructField("start_time", StringType()),
    StructField("lat", DoubleType()),
    StructField("lon", DoubleType()),
    StructField("ingestion_time", StringType())
])


#----------------------------------------------------------#
#                        Main Method                       #
#----------------------------------------------------------#

if __name__ == "__main__":

    spark = create_spark()

    # program_start is the exact time you started this program
    program_start_time = lit(PROGRAM_START_TIME.isoformat()).cast("timestamp")

    #------------------------------------------------------#
    #              Read Streams from Kafka                 #
    #------------------------------------------------------#

    weather_stream = spark.readStream.format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "weather_topic") \
        .load()

    traffic_stream = spark.readStream.format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "traffic_topic") \
        .load()


    #------------------------------------------------------#
    #            Parse and Structure Streams               #
    #------------------------------------------------------#

    weather_stream = weather_stream.select(
        from_json(col("value").cast("string"), weather_schema).alias("data")
    ).select(
        from_unixtime(col("data.timestamp")).cast("timestamp").alias("w_timestamp"),
        to_timestamp(col("data.ingestion_time")).alias("w_event_time"),
        col("data.weather").alias("weather_desc")
    ).withWatermark("w_event_time", "10 minutes")

    weather_stream = weather_stream.withColumn(
        "w_minute", (col("w_event_time").cast("long") / 60).cast("long")
    )


    traffic_stream = traffic_stream.select(
        from_json(col("value").cast("string"), traffic_schema).alias("data")
    ).select(
        col("data.id").alias("t_id"),
        to_timestamp(col("data.start_time"), "yyyy-MM-dd'T'HH:mm:ssX").alias("t_start"),
        to_timestamp(col("data.ingestion_time")).alias("t_event_time")
    )

    # Filter using ingestion time, not incident start time
    traffic_stream = traffic_stream.filter(
        col("t_event_time") >= program_start_time
    )
    
    traffic_stream = traffic_stream \
        .withWatermark("t_event_time", "10 minutes") \
        .dropDuplicates(["t_id", "t_start"])

    traffic_stream = traffic_stream.withColumn(
        "t_minute", (col("t_event_time").cast("long") / 60).cast("long")
    )

    #------------------------------------------------------#
    #                 Join Streams                         #
    #------------------------------------------------------#

    joined_stream = traffic_stream.join(
        weather_stream,
        expr("""
            t_minute = w_minute AND
            t_event_time BETWEEN w_event_time - interval 5 minutes
            AND w_event_time + interval 5 minutes
        """),
        how="leftOuter"
    )

    joined_stream = joined_stream.dropDuplicates(["t_id", "t_event_time"])

    result = joined_stream.select(
    "t_id",
    "t_start",
    "weather_desc"
    )

    weather_count = weather_stream.select(
        "w_timestamp",
        "weather_desc"
    ).dropDuplicates(["w_timestamp"])


    #------------------------------------------------------#
    #               Snowflake Configuration                #
    #------------------------------------------------------#

    # ----- David's Credentials -----
    pkb_string = get_private_key_string("rsa_key.p8")
    
    sf_options = {
        "sfURL": "sfedu02-unb02139.snowflakecomputing.com",
        "sfUser": "JELLYFISH",
        "sfDatabase": "JELLYFISH_DB",
        "sfSchema": "JELLYFISH_SCHEMA",
        "sfWarehouse": "JELLYFISH_WH",
        "pem_private_key": pkb_string
    }
    
    # ----- Angelina's Credentials -----
    # pkb_string = get_private_key_string("rsa_key.p8", password="Kaylee7355!")
    
    # sf_options = {
    #     "sfURL": "sfedu02-unb02139.snowflakecomputing.com",
    #     "sfUser": "PIGEON",
    #     "sfDatabase": "PIGEON_DB",
    #     "sfSchema": "MY_SCHEMA",
    #     "sfWarehouse": "PIGEON_WH",
    #     "sfRole": "TRAINING_ROLE",
    #     "pem_private_key": pkb_string
    # }
    
    #------------------------------------------------------#
    #                 Redis Configuration                  #
    #------------------------------------------------------#

    redis_client = redis.Redis(
        host=os.getenv("REDIS_HOST", "localhost"),
        port=int(os.getenv("REDIS_PORT", "6379")),
        decode_responses=True
    )
    
    #------------------------------------------------------#
    #         Redis Caching for Live Dashboard             #
    #------------------------------------------------------#
    
    def write_dashboard_cache(batch_df):
        """
        Cache latest joined rows + weather counts for low-latency dashboard reads.
        """
        if batch_df.count() == 0:
            return
        
        # Keep latest 100 incidents for table widget
        latest_rows = [
            row.asDict(recursive=True)
            for row in batch_df.orderBy(col("t_start").desc()).limit(100).collect()
        ]
        redis_client.set("dashboard:recent_incidents", json.dumps(latest_rows, default=str), ex=300)
        
        # Incident count by weather for bar chart
        counts_rows = [
            row.asDict(recursive=True)
            for row in batch_df.groupBy("weather_desc").count().collect()
        ]
        redis_client.set("dashboard:incident_counts", json.dumps(counts_rows, default=str), ex=300)

        redis_client.set("dashboard:last_update_ts", datetime.now(UTC).isoformat(), ex=300)

    #------------------------------------------------------#
    #                 Write to Snowflake                   #
    #------------------------------------------------------#

    def write_to_snowflake(df, epoch_id):
        """
        Writes each micro-batch to Snowflake using private key auth.
        """
        if df.count() == 0:
            print(f"[Epoch {epoch_id}] joined batch empty")
            return
        
        batch_rows = df.count()
        print(f"[Epoch {epoch_id}] joined rows: {batch_rows}")

        # Update Redis first for live dashboard responsiveness
        write_dashboard_cache(df)

        try:
            df.write \
                .format("net.snowflake.spark.snowflake") \
                .options(**sf_options) \
                .option("dbtable", "weather_traffic_comb") \
                .mode("append") \
                .save()
        except Exception as e:
            print(f"[Epoch {epoch_id}] Snowflake write failed: {e}")

    def write_weather_duration(df, epoch_id):
        """
        Track duration per unique weather condition.
        """
        df.write \
            .format("net.snowflake.spark.snowflake") \
            .options(**sf_options) \
            .option("dbtable", "weather_duration") \
            .mode("append") \
            .save()


    query = result.writeStream \
        .foreachBatch(write_to_snowflake) \
        .outputMode("append") \
        .option("checkpointLocation", "/tmp/spark_checkpoint") \
        .start()

    weather_query = weather_count.writeStream \
        .foreachBatch(write_weather_duration) \
        .outputMode("append") \
        .option("checkpointLocation", "/tmp/spark_checkpoint_weather") \
        .start()

    query.awaitTermination()
    weather_query.awaitTermination()