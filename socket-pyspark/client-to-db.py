from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, sum, to_timestamp, round
from pyspark.sql.types import StructType, StructField, StringType, FloatType, IntegerType
import mysql.connector


def create_mysql_table():
    try:
        connection = mysql.connector.connect(
            host=MYSQL_HOST,
            port=MYSQL_PORT,
            user=MYSQL_USER,
            password=MYSQL_PASSWORD,
            database=MYSQL_DATABASE,
            auth_plugin=MYSQL_AUTH
        )

        cursor = connection.cursor()

        create_table_queries = [
            """
            CREATE TABLE IF NOT EXISTS summary_1m (
                id INT AUTO_INCREMENT PRIMARY KEY,
                timestamp DATETIME,
                DID INT,
                DSN VARCHAR(255),
                total_cost FLOAT
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS summary_1h (
                id INT AUTO_INCREMENT PRIMARY KEY,
                timestamp DATETIME,
                DID INT,
                DSN VARCHAR(255),
                total_cost FLOAT
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS summary_1d (
                id INT AUTO_INCREMENT PRIMARY KEY,
                timestamp DATETIME,
                DID INT,
                DSN VARCHAR(255),
                total_cost FLOAT
            )
            """
        ]

        for query in create_table_queries:
            cursor.execute(query)

        connection.commit()
        cursor.close()
        connection.close()
        print("MySQL tables have been created successfully.")

    except Exception as e:
        print(f"An error occurred while creating MySQL tables: {str(e)}")


def save_to_mysql(batch_df, batch_id, table_name):
    try:
        connection = mysql.connector.connect(
            host=MYSQL_HOST,
            port=MYSQL_PORT,
            user=MYSQL_USER,
            password=MYSQL_PASSWORD,
            database=MYSQL_DATABASE,
            auth_plugin=MYSQL_AUTH
        )

        cursor = connection.cursor()

        for row in batch_df.collect():
            timestamp = row['timestamp']
            DID = row['DID']
            DSN = row['DSN']
            total_cost = row['total_cost']

            insert_query = f"""
            INSERT INTO {table_name} (timestamp, DID, DSN, total_cost)
            VALUES (%s, %s, %s, %s)
            """

            cursor.execute(insert_query, (timestamp, DID, DSN, total_cost))

        connection.commit()
        cursor.close()
        connection.close()
        print(f"Data successfully written to {table_name}.")

    except Exception as e:
        print(f"An error occurred while writing data to {table_name}: {str(e)}")


def spark_streaming(host='localhost', port=65432):
    schema = StructType([
        StructField("CCD", StringType(), True),
        StructField("COST", FloatType(), True),
        StructField("DID", IntegerType(), True),
        StructField("DSN", StringType(), True),
        StructField("ID", IntegerType(), True),
        StructField("KPWR", FloatType(), True),
        StructField("KPWS", FloatType(), True),
        StructField("KPWT", FloatType(), True),
        StructField("LAMPWH", FloatType(), True),
        StructField("SN", StringType(), True),
        StructField("STIME", StringType(), True)
    ])

    spark = SparkSession.builder.appName("SocketStreaming").getOrCreate()

    lines = spark.readStream.format("socket").option("host", host).option("port", port).load()

    json_df = lines.select(from_json(col("value").cast("string"), schema).alias("data")) \
        .select(to_timestamp("data.STIME").alias("STIME"), "data.DID", "data.DSN", "data.COST")
        
    minute_windowed_df = (json_df
        .withWatermark("STIME", "1 minute")
        .groupBy(window(col("STIME"), "1 minute").alias("Timestamp"), col("DID"), col("DSN"))
        .agg(sum("COST").alias("total_cost"))
        .selectExpr("Timestamp.start AS timestamp", "DID", "DSN", "total_cost"))

    minute_query = (minute_windowed_df.writeStream
        .foreachBatch(lambda df, batch_id: save_to_mysql(df, batch_id, "summary_1m"))
        .outputMode("update")
        .trigger(processingTime="1 minute")
        .start())

    hourly_windowed_df = (json_df
        .withWatermark("STIME", "1 hour")
        .groupBy(window(col("STIME"), "1 hour").alias("Timestamp"), col("DID"), col("DSN"))
        .agg(sum("COST").alias("total_cost"))
        .selectExpr("Timestamp.start AS timestamp", "DID", "DSN", "total_cost"))

    hourly_query = (hourly_windowed_df.writeStream
        .foreachBatch(lambda df, batch_id: save_to_mysql(df, batch_id, "summary_1h"))
        .outputMode("update")
        .trigger(processingTime="1 hour")
        .start())

    daily_windowed_df = (json_df
        .withWatermark("STIME", "1 day")
        .groupBy(window(col("STIME"), "1 day", startTime="").alias("Timestamp"), col("DID"), col("DSN"))
        .agg(sum("COST").alias("total_cost"))
        .selectExpr("Timestamp.start AS timestamp", "DID", "DSN", "total_cost"))

    daily_query = (daily_windowed_df.writeStream
        .foreachBatch(lambda df, batch_id: save_to_mysql(df, batch_id, "summary_1d"))
        .outputMode("update")
        .trigger(processingTime="1 day")
        .start())

    minute_query.awaitTermination()
    hourly_query.awaitTermination()
    daily_query.awaitTermination()




if __name__ == "__main__":
    
    MYSQL_HOST = 'localhost'
    MYSQL_PORT = 3306
    MYSQL_USER = 'your_user'
    MYSQL_PASSWORD = 'your_password'
    MYSQL_DATABASE = 'summary_data'
    MYSQL_AUTH = 'mysql_native_password'

create_mysql_table()

spark_streaming('localhost', 65432)
