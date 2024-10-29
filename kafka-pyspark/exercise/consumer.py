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

    stream_df = (spark.readStream
                .format("kafka")
                .option("kafka.bootstrap.servers", ",".join(bootstrap_servers))
                .option("subscribe", topic_name)
                .option("startingOffsets", "latest") 
                .load())


    parsed_stream_df = stream_df.selectExpr("CAST(value AS STRING) as json") \
                                .select(from_json(col("json"), schema).alias("data")) \
                                .select(("data.*"))
                                
    parsed_stream_df = parsed_stream_df.withColumn("date_time", expr("to_timestamp(date_time)"))

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

    query = (windowed_df_per_5_minutes.writeStream
            .outputMode('update')  
            .format('console')
            .option('truncate', False)
            .trigger(processingTime='300 seconds')  
            .start())

    query.awaitTermination()


if __name__ == "__main__":
    spark_conn = SparkSession.builder.appName("KafkaStreamConsumer")\
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1") \
        .getOrCreate()

    topic_name = 'test_topic'
    bootstrap_servers = ['localhost:9092']

    spark_conn.conf.set("spark.sql.streaming.statefulOperator.checkCorrectness.enabled", "false")

    try:
        start_streaming(spark_conn, topic_name, bootstrap_servers)
    except Exception as e:
        print("An error occurred while streaming:", str(e))

