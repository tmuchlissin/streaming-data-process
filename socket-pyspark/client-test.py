import socket
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, FloatType, IntegerType, TimestampType
from pyspark.sql.functions import from_json, col

def start_socket_consumer(spark, host, port):
    schema = StructType([
        StructField("STIME", StringType()),
        StructField("DID", IntegerType()),
        StructField("DSN", StringType()),
        StructField("ID", IntegerType()),
        StructField("SN", StringType()),
        StructField("COST", FloatType()),
        StructField("LAMPWH", IntegerType()),
        StructField("CCD", StringType()),
        StructField("KPWR", IntegerType()),
        StructField("KPWS", IntegerType()),
        StructField("KPWT", IntegerType())
    ])


    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind((host, port))
    s.listen(1)
    print(f"Waiting for connection from Go program at {host}:{port}...")

    conn, addr = s.accept()
    print(f"Accepted connection from {addr}")

    try:
        lines = spark.readStream.format("socket").option("host", host).option("port", port).load()


        parsed_df = lines.select(from_json(col('value'), schema).alias("data")).select(("data.*"))

        query = (parsed_df.writeStream
                .outputMode("append")
                .format("console")
                .option("truncate", False)
                .start())


        query.awaitTermination()

    except Exception as e:
        print("An error occurred while consuming:", str(e))
    finally:
        conn.close()
        spark.stop()

if __name__ == "__main__":
    spark = SparkSession.builder.appName("SocketStreamConsumer").getOrCreate()

    host = "localhost"
    port = 9191

    try:
        start_socket_consumer(spark, host, port)
    except Exception as e:
        print("An error occurred:", str(e))


