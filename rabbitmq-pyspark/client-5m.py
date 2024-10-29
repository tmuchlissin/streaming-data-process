# streaming.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, sum, avg, window, current_timestamp, expr, date_format, round
from pyspark.sql.types import StructType, StructField, StringType, FloatType, IntegerType, TimestampType

def start_streaming(spark, host='localhost', port=9999):
    schema = StructType([
        StructField("date_time", StringType()),
        StructField("id", StringType()),
        StructField("city", StringType()),
        StructField("P (watt)", IntegerType()),
        StructField("V (volt)", IntegerType()),
        StructField("I (amphere)", FloatType())
    ])

    stream_df = (spark.readStream
                .format("socket") 
                .option("host", host)
                .option("port", port)
                .load())
 
    # Parsing JSON data from the socket
    stream_df = stream_df.select(from_json(col('value'), schema).alias("data")).select(("data.*"))

    # Convert the date_time column to timestamp type
    stream_df = stream_df.withColumn("date_time", expr("to_timestamp(date_time)"))

    # Windowing operation and aggregation per 1 minute
    windowed_df_per_minute = stream_df \
        .select("date_time", "id", "city", "P (watt)", "V (volt)", "I (amphere)") \
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

    # Set spark configuration to disable consistency check
    spark.conf.set("spark.sql.streaming.statefulOperator.checkCorrectness.enabled", "false")

    # Displaying the streaming summary data in the console
    query = (windowed_df_per_5_minutes.writeStream
            .outputMode('update')  # Changing the output mode to "update"
            .format('console')
            .option('truncate', False)
            .trigger(processingTime='300 seconds')  # Set trigger to process every 5 minutes (300 seconds)
            .start())

    # Await termination to keep the stream running
    query.awaitTermination()



if __name__ == "__main__":
    spark_conn = SparkSession.builder.appName("SocketStreamConsumer").getOrCreate()
    start_streaming(spark_conn, host='localhost', port=9999)
