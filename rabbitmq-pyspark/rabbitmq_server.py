import json
import pandas as pd
from datetime import datetime
import random
import pika
import time

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
            anomaly_value_watt = random.choice(
                [random.randint(200, 250), random.randint(300, 350)])
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


def send_data_over_rabbitmq(interval_seconds=10, credentials=None, parameters=None):
    if credentials is None or parameters is None:
        raise ValueError("Credentials and parameters must be provided")

    connection = pika.BlockingConnection(parameters)
    channel = connection.channel()
    channel.queue_declare(queue='dummy_data')

    try:
        while True:
            dummy_data = generate_dummy_data(chunk_size=5)
            chunk = pd.DataFrame(dummy_data)
            json_data = chunk.to_json(orient='records')

            channel.basic_publish(
                exchange='', routing_key='dummy_data', body=json_data)
            print(json_data)

            time.sleep(interval_seconds)

    except KeyboardInterrupt:
        connection.close()


if __name__ == "__main__":
    credentials = pika.PlainCredentials('admin', 'admin')
    parameters = pika.ConnectionParameters(
        host='localhost', port=5672, virtual_host='smartsiklon_broker', credentials=credentials)
    send_data_over_rabbitmq(interval_seconds=10,
                            credentials=credentials, parameters=parameters)
