from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, sum, avg, window, expr, date_format, round
from pyspark.sql.types import StructType, StructField, StringType, FloatType, IntegerType, TimestampType
from kafka import KafkaConsumer
import json

def start_streaming(spark, topic_name, bootstrap_servers):
    schema = StructType([
        StructField("date_time", StringType()),
        StructField("id", StringType()),
        StructField("city", StringType()),
        StructField("P (watt)", IntegerType()),
        StructField("V (volt)", IntegerType()),
        StructField("I (amphere)", FloatType())
    ])

    # Ubah format bacaan menjadi "kafka" untuk membaca dari Kafkaubscribe", topic_name)
    stream_df = (spark.readStream
                .format("kafka")
                .option("kafka.bootstrap.servers", ",".join(bootstrap_servers))
                .option("subscribe", topic_name)
                .option("startingOffsets", "earliest")  # Tambahkan opsi startingOffsets
                .load())


    # Parsing JSON data dari Kafka
    parsed_stream_df = stream_df.selectExpr("CAST(value AS STRING) as json") \
                                .select(from_json(col("json"), schema).alias("data")) \
                                .select(("data.*"))

    # Ubah kolom 'date_time' menjadi timestamp
    parsed_stream_df = parsed_stream_df.withColumn("date_time", expr("to_timestamp(date_time)"))

    # Windowing operation and aggregation per 1 minute
    windowed_df_per_minute = parsed_stream_df \
        .withWatermark("date_time", "1 minute") \
        .groupBy(window(col("date_time"), "1 minute").alias("timestamp"), col("id"), col("city")) \
        .agg(
            sum("P (watt)").alias("total_watt"),
            sum("V (volt)").alias("total_volt"),
            round(sum("I (amphere)"), 2).alias("total_amphere"),
            round(avg("P (watt)"), 2).alias("avg_watt"),
            round(avg("V (volt)"), 2).alias("avg_volt"),
            round(avg("I (amphere)"), 2).alias("avg_amphere")
        ) \
        .withColumn("timestamp", date_format(col("timestamp.start"), "yyyy-MM-dd HH:mm:ss"))

    # Windowing operation and aggregation per 5 minutes
    windowed_df_per_5_minutes = windowed_df_per_minute \
        .groupBy(window(col("timestamp"), "5 minutes").alias("timestamp"), col("id"), col("city")) \
        .agg(
            sum("total_watt").alias("total_watt"),
            sum("total_volt").alias("total_volt"),
            round(sum("total_amphere"),2).alias("total_amphere"),
            round(avg("avg_watt"), 2).alias("avg_watt"),
            round(avg("avg_volt"), 2).alias("avg_volt"),
            round(avg("avg_amphere"), 2).alias("avg_amphere")
        ) \
        .withColumn("timestamp", date_format(col("timestamp.start"), "yyyy-MM-dd HH:mm:ss"))

    # Tampilkan data dalam konsol
    query = (windowed_df_per_5_minutes.writeStream
            .outputMode('update')  # Changing the output mode to "update"
            .format('console')
            .option('truncate', False)
            .trigger(processingTime='300 seconds')  # Set trigger to process every 5 minutes (300 seconds)
            .start())

    # Tunggu hingga streaming selesai
    query.awaitTermination()


if __name__ == "__main__":
    # Inisialisasi SparkSession
    spark_conn = SparkSession.builder.appName("KafkaStreamConsumer")\
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1") \
        .getOrCreate()

    # Tentukan konfigurasi Kafka
    topic_name = 'test_topic_v2'
    bootstrap_servers = ['localhost:9092']

    # Tambahkan konfigurasi untuk menonaktifkan pemeriksaan kebenaran data untuk operasi stateful
    spark_conn.conf.set("spark.sql.streaming.statefulOperator.checkCorrectness.enabled", "false")

    try:
        # Mulai streaming
        start_streaming(spark_conn, topic_name, bootstrap_servers)
    except Exception as e:
        print("An error occurred while streaming:", str(e))
