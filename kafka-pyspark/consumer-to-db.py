from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, FloatType, IntegerType, TimestampType
from pyspark.sql.functions import from_json, col, sum, window
from pymongo import MongoClient
import mysql.connector

def create_mysql_table():
    try:
        connection = mysql.connector.connect(
            host="ip_address",
            user="your_user",
            password="your_password",
            database="summary_kafka",
            auth_plugin='mysql_native_password'
        )

        cursor = connection.cursor()

        create_table_queries = [
            """
            CREATE TABLE IF NOT EXISTS summary_kafka_1m (
                id INT AUTO_INCREMENT PRIMARY KEY,
                timestamp DATETIME,
                DID INT,
                DSN VARCHAR(255),
                total_cost FLOAT
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS summary_kafka_1h (
                id INT AUTO_INCREMENT PRIMARY KEY,
                timestamp DATETIME,
                DID INT,
                DSN VARCHAR(255),
                total_cost FLOAT
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS summary_kafka_1d (
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
        connection.close()
        print("MySQL tables have been created successfully.")

    except Exception as e:
        print(f"An error occurred while creating MySQL tables: {str(e)}")


def save_to_mysql(batch_df, batch_id, table_name):
    try:
        connection = mysql.connector.connect(
            host="ip_address",
            user="your_user",
            password="your_password",
            database="summary_kafka",
            auth_plugin='mysql_native_password'
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
        connection.close()
        print(f"Data successfully written to {table_name}.")

    except Exception as e:
        print(f"An error occurred while writing data to {table_name}: {str(e)}")


def create_mongodb_collection(db_name, collection_name, mongo_uri='mongodb://localhost:27017/'):
    try:
        # Connection to MongoDB
        client = MongoClient(mongo_uri)
        db = client[db_name]
        
        # Create collection if not exists
        if collection_name not in db.list_collection_names():
            db.create_collection(collection_name)
            print(f"MongoDB collection '{collection_name}' has been created successfully in database '{db_name}'.")
        else:
            print(f"MongoDB collection '{collection_name}' already exists in database '{db_name}'.")

    except Exception as e:
        print(f"An error occurred while creating MongoDB collection '{collection_name}' in database '{db_name}': {str(e)}")


def save_to_mongodb(batch_df, batch_id, collection_name):
    try:
        # Connection to MongoDB
        client = MongoClient("mongodb://localhost:27017/")
        db = client["summary_kafka"]
        collection = db[collection_name]

        # Iterate over each row in the DataFrame and insert into MongoDB
        for row in batch_df.collect():
            document = row.asDict()  # Convert row to dictionary
            collection.insert_one(document)  # Insert document into MongoDB

        print(f"Data successfully written to MongoDB collection '{collection_name}'.")

    except Exception as e:
        print(f"An error occurred while writing data to MongoDB collection '{collection_name}': {str(e)}")


def start_streaming(spark, topic_name, bootstrap_servers):

    schema = StructType([
        StructField("CCD", StringType()),
        StructField("COST", FloatType()),
        StructField("DID", IntegerType()),
        StructField("DSN", StringType()),
        StructField("ID", IntegerType()),
        StructField("KPWR", IntegerType()),
        StructField("KPWS", IntegerType()),
        StructField("KPWT", IntegerType()),
        StructField("LAMPWH", IntegerType()),
        StructField("SN", StringType()),
        StructField("STIME", StringType())
    ])

    # Create MongoDB collection for raw data
    create_mongodb_collection("summary_kafka", "summary_raw_data")

    stream_df = (spark.readStream
                .format("kafka")
                .option("kafka.bootstrap.servers", ",".join(bootstrap_servers))
                .option("subscribe", topic_name)
                .option("startingOffsets", "latest")
                .load())

    parsed_stream_df = (stream_df.selectExpr("CAST(value AS STRING) as json")
                        .select(from_json(col("json"), schema).alias("data"))
                        .select("data.*"))

    # Ubah kolom STIME menjadi tipe Timestamp
    parsed_stream_df = parsed_stream_df.withColumn("STIME", col("STIME").cast(TimestampType()))

    # Simpan data mentah ke MongoDB
    raw_data_query = (parsed_stream_df.writeStream
                    .foreachBatch(lambda df, batch_id: save_to_mongodb(df, batch_id, "summary_raw_data"))
                    .outputMode("append")  # Mode append untuk menyimpan data baru ke MongoDB
                    .start())

    # Summary per 1 Minute
    minute_windowed_df = (parsed_stream_df
                        .withWatermark("STIME", "1 minute")
                        .groupBy(window(col("STIME"), "1 minute").alias("Timestamp"), col("DID"), col("DSN"))
                        .agg(sum("COST").alias("total_cost"))
                        .selectExpr("Timestamp.start AS timestamp", "DID", "DSN", "total_cost"))

    minute_query = (minute_windowed_df.writeStream
                    .foreachBatch(lambda df, batch_id: save_to_mysql(df, batch_id, "summary_kafka_1m"))
                    .outputMode("update")
                    .trigger(processingTime="1 minute")
                    .start())

    # Summary per 1 Hour
    hourly_windowed_df = (parsed_stream_df
                        .withWatermark("STIME", "1 hour")
                        .groupBy(window(col("STIME"), "1 hour").alias("Timestamp"), col("DID"), col("DSN"))
                        .agg(sum("COST").alias("total_cost"))
                        .selectExpr("Timestamp.start AS timestamp", "DID", "DSN", "total_cost"))

    hourly_query = (hourly_windowed_df.writeStream
                    .foreachBatch(lambda df, batch_id: save_to_mysql(df, batch_id, "summary_kafka_1h"))
                    .outputMode("update")
                    .trigger(processingTime="1 hour")
                    .start())

    # Summary per 1 Day
    daily_windowed_df = (parsed_stream_df
                        .withWatermark("STIME", "1 day")
                        .groupBy(window(col("STIME"), "1 day").alias("Timestamp"), col("DID"), col("DSN"))
                        .agg(sum("COST").alias("total_cost"))
                        .selectExpr("Timestamp.start AS timestamp", "DID", "DSN", "total_cost"))

    daily_query = (daily_windowed_df.writeStream
                .foreachBatch(lambda df, batch_id: save_to_mysql(df, batch_id, "summary_kafka_1d"))
                .outputMode("update")
                .trigger(processingTime="1 day")
                .start())

    raw_data_query.awaitTermination()
    minute_query.awaitTermination()
    hourly_query.awaitTermination()
    daily_query.awaitTermination()


if __name__ == "__main__":
    spark_conn = SparkSession.builder.appName("KafkaStreamConsumer") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1") \
        .getOrCreate()

    # Create table if doesn't exist
    create_mysql_table()

    topic_name = 'example-device-data'
    bootstrap_servers = ['ip_address:9092']

    try:
        start_streaming(spark_conn, topic_name, bootstrap_servers)
    except Exception as e:
        print("An error occurred while streaming:", str(e))
