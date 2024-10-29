from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, FloatType, IntegerType, TimestampType
from pyspark.sql.functions import from_json, col, sum, window, expr, date_format, round
from kafka import KafkaConsumer
import json

def start_streaming(spark, topic_name, bootstrap_servers):
    schema = StructType([
        StructField("CCD", StringType()),
        StructField("COST", FloatType()),
        StructField("DID", IntegerType()),
        StructField("DSN", StringType()),
        StructField("ID", IntegerType()),
        StructField("KPWR", IntegerType()),
        StructField("KPWS", IntegerType()),
        StructField("KPWT", IntegerType()),
        StructField("LAMPWH", IntegerType()),
        StructField("SN", StringType()),
        StructField("STIME", StringType())
    ])  

    stream_df = (spark.readStream
                .format("kafka")
                .option("kafka.bootstrap.servers", ",".join(bootstrap_servers))
                .option("subscribe", topic_name)
                .option("startingOffsets", "latest") 
                .load())

    parsed_stream_df = stream_df.selectExpr("CAST(value AS STRING) as json") \
                                .select(from_json(col("json"), schema).alias("data")) \
                                .select("data.*")

    parsed_stream_df = parsed_stream_df.withColumn("STIME", col("STIME").cast(TimestampType())) \
                                    .withWatermark("STIME", "1 minute") 

    windowed_df = parsed_stream_df \
        .groupBy(window(col("STIME"), "1 minute").alias("Timestamp"), col("DID"), col("DSN")) \
        .agg(round(sum("COST"),2).alias("total_cost")) \
        .selectExpr("Timestamp.start AS timestamp", "DID", "DSN", "total_cost")  # Mengubah nama kolom 'start' menjadi 'timestamp'

    query = (windowed_df.writeStream
            .outputMode("update")
            .format("console")
            .option("truncate", False)
            .trigger(processingTime="1 minute") 
            .start())

    query.awaitTermination()


if __name__ == "__main__":
    spark_conn = SparkSession.builder.appName("KafkaStreamConsumer") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1") \
        .getOrCreate()

    topic_name = 'example-device-data'
    bootstrap_servers = ['ip_address:9092']

    try:
        start_streaming(spark_conn, topic_name, bootstrap_servers)
    except Exception as e:
        print("An error occurred while streaming:", str(e))
