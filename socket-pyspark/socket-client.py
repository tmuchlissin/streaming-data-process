from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, sum, avg, window, expr, date_format, round
from pyspark.sql.types import StructType, StructField, StringType, FloatType, IntegerType, TimestampType
import mysql.connector

def create_mysql_table():
    try:
        # Membuat koneksi ke MySQL
        connection = mysql.connector.connect(host='localhost',
                                            user='your_user',
                                            password='your_password',
                                            database='summary_data',
                                            auth_plugin='mysql_native_password')

        # Membuat objek cursor
        cursor = connection.cursor()

        # Perintah SQL untuk membuat tabel Summary Data
        create_table_query = """
        CREATE TABLE IF NOT EXISTS summary_table (
            Id INT AUTO_INCREMENT PRIMARY KEY,
            Timestamp DATETIME,
            Id_sensor VARCHAR(255),
            City VARCHAR(255),
            Total_watt INT,
            Total_volt INT,
            Total_amphere FLOAT,
            Avg_watt FLOAT,
            Avg_volt FLOAT,
            Avg_amphere FLOAT
        )
        """
 
        cursor.execute(create_table_query)
        print("Table Summary Data created successfully")

    except mysql.connector.Error as error:
        print("Failed to create MySQL table {}".format(error))

    finally:

        if 'connection' in locals() and connection.is_connected():
            cursor.close()
            connection.close()
            print("MySQL connection is closed")

def start_streaming_to_mysql(spark, host='localhost', port=9999):
    
    # Create table in MySQL
    create_mysql_table() 

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

    # Send the streaming summary data to MySQL
    def save_to_mysql(batch_df, batch_id):
        try:
            # Create Connection to MySQL
            connection = mysql.connector.connect(host='localhost',
                                                user='your_user',
                                                password='your_password',
                                                database='summary_data',
                                                auth_plugin='mysql_native_password')

            
            cursor = connection.cursor()

            # Insert data
            def save_row(row):
                insert_tuple = (row["timestamp"], row["id"], row["city"], row["total_watt"], row["total_volt"], row["total_amphere"], row["avg_watt"], row["avg_volt"], row["avg_amphere"])
                insert_query = f"INSERT INTO summary_table (Timestamp, Id_sensor, City, Total_watt, Total_volt, Total_amphere, Avg_watt, Avg_volt, Avg_amphere) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)"
                cursor.execute(insert_query, insert_tuple)

            for row in batch_df.collect():
                save_row(row)

            # Commit for saving changes
            connection.commit()
            print("Data inserted successfully")

        except mysql.connector.Error as error:
            print("Failed to insert into MySQL table {}".format(error))
            if 'connection' in locals():
                connection.rollback()

        finally:
            if 'connection' in locals() and connection.is_connected():
                cursor.close()
                connection.close()
                print("MySQL connection is closed")

    # Set spark configuration to disable consistency check
    spark.conf.set("spark.sql.streaming.statefulOperator.checkCorrectness.enabled", "false")
    
    # Writing the streaming data to MySQL
    query = (windowed_df_per_5_minutes.writeStream
            .format('console')
            .option('truncate', False)
            .foreachBatch(save_to_mysql)
            .outputMode('update')
            .trigger(processingTime='300 seconds')
            .start()
            )

    # Await termination to keep the stream running
    query.awaitTermination()


if __name__ == "__main__":
    spark_conn = SparkSession.builder.appName("SocketStreamConsumer").getOrCreate()
    start_streaming_to_mysql(spark_conn, host='localhost', port=9999)
