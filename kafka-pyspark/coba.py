from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, FloatType, IntegerType
from pyspark.sql.functions import from_json, col
from confluent_kafka import Consumer, KafkaError
import json

# Definisi skema untuk DataFrame Spark
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

def process_kafka_messages():
    # Inisialisasi SparkSession
    spark = SparkSession.builder \
        .appName("KafkaStreamingToDataFrame") \
        .getOrCreate()
    
    # Konfigurasi Kafka Consumer
    topic_name = 'example-device-data'
    bootstrap_servers = ['ip_address:9092']
    group_id = 'my-consumer-group3'

    conf = {
        'bootstrap.servers': ','.join(bootstrap_servers),
        'group.id': group_id,
        'auto.offset.reset': 'earliest'
    }

    consumer = Consumer(conf)
    consumer.subscribe([topic_name])

    # Buat DataFrame kosong dengan skema yang ditentukan
    df = spark.createDataFrame(spark.sparkContext.emptyRDD(), schema)

    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    print(f'Consume error: {msg.error()}')
                    break
            
            value = msg.value().decode("utf-8") if msg.value() else None
            
            if value:
                # Parse JSON string to Python dictionary
                data_dict = json.loads(value)
                
                # Convert relevant fields to correct types
                data_dict['COST'] = float(data_dict.get('COST', 0.0))  # Convert COST to float
                
                # Create DataFrame from single row dictionary
                new_row = spark.createDataFrame([data_dict], schema)
                
                # Append new row to existing DataFrame
                df = df.union(new_row)

                # Show updated DataFrame
                df.show(truncate=False)

    except KeyboardInterrupt:
        pass

    finally:
        consumer.close()
        spark.stop()

if __name__ == "__main__":
    process_kafka_messages()

