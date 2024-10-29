import mysql.connector
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, sum, avg, window, expr, date_format, round
from pyspark.sql.types import StructType, StructField, StringType, FloatType, IntegerType, TimestampType


# Fungsi untuk membuat koneksi ke database MySQL dan membuat tabel
def create_mysql_table():
    # Menghubungkan ke database MySQL
    connection = mysql.connector.connect(
        host="localhost",
        user="your_user",
        password="your_password",
        database="summary_data",
        auth_plugin='mysql_native_password'
    )

    cursor = connection.cursor()

    # SQL query untuk membuat tabel jika belum ada
    create_table_query = """
    CREATE TABLE IF NOT EXISTS summary_kafka_1m (
        id INT AUTO_INCREMENT PRIMARY KEY,
        timestamp DATETIME,
        city VARCHAR(255),
        total_watt INT,
        total_volt INT,
        total_amphere FLOAT,
        avg_watt FLOAT,
        avg_volt FLOAT,
        avg_amphere FLOAT
    )
    """

    # Eksekusi query membuat tabel
    cursor.execute(create_table_query)

    # SQL query untuk membuat tabel jika belum ada
    create_table_query = """
    CREATE TABLE IF NOT EXISTS summary_kafka_5m (
        id INT AUTO_INCREMENT PRIMARY KEY,
        timestamp DATETIME,
        city VARCHAR(255),
        total_watt INT,
        total_volt INT,
        total_amphere FLOAT,
        avg_watt FLOAT,
        avg_volt FLOAT,
        avg_amphere FLOAT
    )
    """

    # Eksekusi query membuat tabel
    cursor.execute(create_table_query)
    
    # Commit perubahan dan tutup koneksi
    connection.commit()
    connection.close()


def save_to_mysql(batch_df, batch_id, table_name):
    try:
        # Menghubungkan ke database MySQL
        connection = mysql.connector.connect(
            host="localhost",
            user="your_user",
            password="your_password",
            database="summary_data",
            auth_plugin='mysql_native_password'
        )

        cursor = connection.cursor()

        # Iterasi melalui setiap baris dalam DataFrame batch
        for row in batch_df.collect():
            timestamp = row['timestamp']
            city = row['city']
            total_watt = row['total_watt']
            total_volt = row['total_volt']
            total_amphere = row['total_amphere']
            avg_watt = row['avg_watt']
            avg_volt = row['avg_volt']
            avg_amphere = row['avg_amphere']

            # SQL query untuk INSERT INTO
            insert_query = f"""
            INSERT INTO {table_name} (timestamp, city, total_watt, total_volt, total_amphere, avg_watt, avg_volt, avg_amphere)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """

            # Eksekusi query INSERT INTO
            cursor.execute(insert_query, (timestamp, city, total_watt, total_volt, total_amphere, avg_watt, avg_volt, avg_amphere))

        # Commit perubahan dan tutup koneksi
        connection.commit()
        connection.close()
        # Display log message if successful
        print(f"Data successfully written to MySQL table: {table_name}")

    except Exception as e:
        # Display log message if an error occurs
        print(f"An error occurred while writing data to MySQL table {table_name}: {str(e)}")


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
        .groupBy(window(col("date_time"), "1 minute").alias("timestamp"), col("city")) \
        .agg(
            sum("P (watt)").alias("total_watt"),
            sum("V (volt)").alias("total_volt"),
            round(sum("I (amphere)"), 2).alias("total_amphere"),
            round(avg("P (watt)"), 2).alias("avg_watt"),
            round(avg("V (volt)"), 2).alias("avg_volt"),
            round(avg("I (amphere)"), 2).alias("avg_amphere")
        ) \
        .withColumn("timestamp", date_format(col("timestamp.start"), "yyyy-MM-dd HH:mm:ss"))

    windowed_df_per_5_minutes = parsed_stream_df \
        .withWatermark("date_time", "5 minutes") \
        .groupBy(window(col("date_time"), "5 minutes").alias("timestamp"), col("city")) \
        .agg(
            sum("P (watt)").alias("total_watt"),
            sum("V (volt)").alias("total_volt"),
            round(sum("I (amphere)"), 2).alias("total_amphere"),
            round(avg("P (watt)"), 2).alias("avg_watt"),
            round(avg("V (volt)"), 2).alias("avg_volt"),
            round(avg("I (amphere)"), 2).alias("avg_amphere")
        ) \
        .withColumn("timestamp", date_format(col("timestamp.start"), "yyyy-MM-dd HH:mm:ss"))

    # Write data to MySQL for both 1 minute and 5 minutes summaries
    query_minute = (windowed_df_per_minute.writeStream
                    .foreachBatch(lambda df, id: save_to_mysql(df, id, "summary_kafka_1m"))
                    .outputMode('update')
                    .trigger(processingTime='60 seconds')
                    .start())

    query_5_minutes = (windowed_df_per_5_minutes.writeStream
                    .foreachBatch(lambda df, id: save_to_mysql(df, id, "summary_kafka_5m"))
                    .outputMode('update')
                    .trigger(processingTime='300 seconds')
                    .start())

    query_minute.awaitTermination()
    query_5_minutes.awaitTermination()


if __name__ == "__main__":
    # Inisialisasi SparkSession
    spark_conn = SparkSession.builder.appName("KafkaStreamConsumer") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1") \
        .getOrCreate()

    # Buat tabel MySQL jika belum ada
    create_mysql_table()

    # Tentukan konfigurasi Kafka
    topic_name = 'test_topic'
    bootstrap_servers = ['localhost:9092']

    # Tambahkan konfigurasi untuk menonaktifkan pemeriksaan kebenaran data untuk operasi stateful
    spark_conn.conf.set("spark.sql.streaming.statefulOperator.checkCorrectness.enabled", "false")

    try:
        # Mulai streaming
        start_streaming(spark_conn, topic_name, bootstrap_servers)
    except Exception as e:
        print("An error occurred while streaming:", str(e))


