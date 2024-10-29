from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType
import socket

# Inisialisasi SparkSession
spark = SparkSession.builder \
    .appName("SocketStreamExample") \
    .getOrCreate()

# Definisikan skema untuk data streaming
schema = StructType([
    StructField("date_time", StringType()),
    StructField("id", StringType()),
    StructField("city", StringType()),
    StructField("P (watt)", IntegerType()),
    StructField("V (volt)", IntegerType()),
    StructField("I (amphere)", FloatType())
])

# Baca data streaming dari socket
socket_df = spark \
    .readStream \
    .format("socket") \
    .option("host", "localhost") \
    .option("port", 9999) \
    .load()

# Parse data JSON
parsed_df = socket_df.select(from_json(col("value"), schema).alias("data")).select("data.*")

# Lakukan transformasi atau operasi lainnya pada DataFrame
# Misalnya, lakukan operasi agregasi atau filter

# Tulis DataFrame ke sink
query = parsed_df \
    .writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

# Tunggu proses streaming selesai
query.awaitTermination()
