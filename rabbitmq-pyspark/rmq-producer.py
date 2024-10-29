import pika
import json
import time
from datetime import datetime
import random
import pandas as pd 

# Daftar kota beserta ID-nya
CITIES = {
    1: 'Surabaya',
    2: 'Sidoarjo',
    3: 'Gresik',
    4: 'Malang',
    5: 'Pasuruan'
} 

# Fungsi untuk membuat koneksi RabbitMQ
def create_rabbitmq_connection():
    credentials = pika.PlainCredentials('guest', 'guest')
    connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost', credentials=credentials))
    channel = connection.channel()
    return channel

# Fungsi untuk menghasilkan data dummy untuk setiap kota
def generate_dummy_data():
    data = []
    for city_id, city_name in CITIES.items():
        # Generate dummy data for each city
        watt_value = random.randint(250, 300)
        voltage_value = random.randint(100, 200)
        electric_current = round((watt_value / voltage_value), 2)
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        # Data for the current city 
        city_data = {
            'date_time': timestamp,
            'id': str(city_id),
            'city': city_name,
            'P (watt)': watt_value,
            'V (volt)': voltage_value,
            'I (amphere)': electric_current
        }
        data.append(city_data)
    return data

# Fungsi untuk mengirim pesan data sensor ke RabbitMQ
def send_sensor_data_to_rabbitmq(channel):
    data = generate_dummy_data()
    df = pd.DataFrame(data)
    print(df)

    for city_data in data:
        message = json.dumps(city_data)
        channel.basic_publish(exchange='sensor_data', routing_key=f'sensor_data.{city_data["id"]}', body=message)

# Membuat koneksi RabbitMQ
channel = create_rabbitmq_connection()
channel.exchange_declare(exchange='sensor_data', durable=True, exchange_type='topic')
queue_name = 'sensor_data_queue'
channel.queue_declare(queue=queue_name)
channel.queue_bind(exchange='sensor_data', queue=queue_name, routing_key='sensor_data.*')

# Mengirim pesan data sensor ke RabbitMQ
while True:
    try:
        send_sensor_data_to_rabbitmq(channel)
        time.sleep(10)  # Mengatur interval waktu pengiriman pesan
    except KeyboardInterrupt:
        break

# Menutup koneksi RabbitMQ setelah pengiriman pesan selesai
channel.close()
