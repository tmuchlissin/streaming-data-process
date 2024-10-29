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

    stream_df = stream_df.select(from_json(col('value'), schema).alias("data")).select(("data.*"))

    stream_df = stream_df.withColumn("date_time", expr("to_timestamp(date_time)"))

    windowed_df = stream_df \
        .select("date_time", "id", "city", "P (watt)", "V (volt)", "I (amphere)") \
        .withWatermark("date_time", "1 minute") \
        .groupBy(window(col("date_time"), "1 minute").alias("window"), col("id"), col("city")) \
        .agg(
            sum("P (watt)").alias("total_watt"),
            sum("V (volt)").alias("total_volt"),
            round(sum("I (amphere)"), 2).alias("total_amphere"),
            round(avg("P (watt)"), 2).alias("avg_watt"),
            round(avg("V (volt)"), 2).alias("avg_volt"),
            round(avg("I (amphere)"), 2).alias("avg_amphere")
        ) \
        .withColumn("window", date_format(col("window.start"), "yyyy-MM-dd HH:mm:ss"))

    query = (windowed_df.writeStream
            .outputMode('update')  
            .format('console')
            .option('truncate', False)
            .trigger(processingTime='60 seconds')
            .start())

    query.awaitTermination()

if __name__ == "__main__":
    spark_conn = SparkSession.builder.appName("SocketStreamConsumer").getOrCreate()
    start_streaming(spark_conn, host='localhost', port=9999)
