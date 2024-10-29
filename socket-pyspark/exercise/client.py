# streaming.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, FloatType, IntegerType

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

    # Displaying the streaming data in the console
    query = (stream_df.writeStream
            .outputMode('')
            .format('console')
            .option('truncate', False)
            .start())

    # Await termination to keep the stream running
    query.awaitTermination()

if __name__ == "__main__":
    spark_conn = SparkSession.builder.appName("SocketStreamConsumer").getOrCreate()
    start_streaming(spark_conn, host='localhost', port=9999)

