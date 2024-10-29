from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType
import socket

# Inisialisasi SparkSession
spark = SparkSession.builder \
    .appName("SocketDataStreaming") \
    .config("spark.sql.shuffle.partitions", "2") \
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

# Fungsi untuk menerima data dari socket server dan mengubahnya menjadi DataFrame
def receive_and_process_data():
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(('localhost', 9999))
        s.listen()
        conn, addr = s.accept()
        with conn:
            print('Connected by', addr)
            while True:
                data = conn.recv(1024)
                if not data:
                    break
                json_data = data.decode()
                process_data(json_data)

# Fungsi untuk memproses data dan mengonversinya menjadi DataFrame
def process_data(json_data):
    try:
        # Parse JSON data
        df = spark.read.json(spark.sparkContext.parallelize([json_data]), schema=schema)
        # Tampilkan DataFrame ke konsol
        df.writeStream \
            .outputMode("append") \
            .format("console") \
            .start() \
            .awaitTermination() 
        # Lakukan operasi lainnya pada DataFrame, seperti menyimpannya ke penyimpanan data lainnya
        # Misalnya:
        # df.write.format("parquet").mode("append").save("/path/to/save/data")
    except Exception as e:
        print(f"Error processing data: {e}")

# Menerima dan memproses data dari socket server
print('Listening on port 9999...')
receive_and_process_data()

# Memulai proses streaming (tidak akan sampai ke sini karena receive_and_process_data() adalah loop yang tak berujung)
spark.streams.awaitAnyTermination()
