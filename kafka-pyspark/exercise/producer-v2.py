import json
import time
from kafka import KafkaProducer
from datetime import datetime
import random

CITIES = {
    1: 'Surabaya',
    2: 'Sidoarjo',
    3: 'Gresik',
    4: 'Malang',
    5: 'Pasuruan'
}

def generate_dummy_data(chunk_size=5, anomaly_probability=0.05):
    unique_city_ids = random.sample(range(1, 6), min(chunk_size, 5))
    dummy_data = []

    for city_id in unique_city_ids:
        generate_anomaly = random.uniform(0, 1) < anomaly_probability

        if generate_anomaly:
            anomaly_value_watt = random.choice([random.randint(200, 250), random.randint(300, 350)])
            watt_value = anomaly_value_watt
        else:
            watt_value = random.randint(250, 300)

        voltage_value = random.randint(100, 200)
        electric_current = round((watt_value / voltage_value), 2)

        data_point = {
            'date_time': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'id': f'{city_id}',
            'city': CITIES[city_id],
            'P (watt)': watt_value,
            'V (volt)': voltage_value,
            'I (amphere)': electric_current
        }
        dummy_data.append(data_point)

    return dummy_data

def send_data_to_kafka(producer, topic_name):
    while True:
        dummy_data = generate_dummy_data(chunk_size=5)
        for data_point in dummy_data:
            producer.send(topic_name, value=data_point)
            print("Data diproduksi:", data_point)
        time.sleep(10)  # Ubah interval pengiriman jika diperlukan

if __name__ == "__main__":
    topic_name = 'test_topic_v2'
    bootstrap_servers = ['localhost:9092']

    producer = KafkaProducer(bootstrap_servers=bootstrap_servers,
                            value_serializer=lambda v: json.dumps(v).encode('utf-8'))

    send_data_to_kafka(producer, topic_name)
