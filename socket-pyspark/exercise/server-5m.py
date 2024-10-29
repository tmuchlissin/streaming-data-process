#server.py
import json
import socket
import time
import pandas as pd
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
        
        electric_current = round((watt_value / voltage_value),2)
        
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


def handle_date(obj):
    if isinstance(obj, pd.Timestamp):
        return obj.strftime('%Y-%m-%d %H:%M:%S')
    raise TypeError("Object of type '%s' is not JSON serializable" % type(obj).__name__)
    
def send_data_over_socket(host='localhost', port=9991, interval_seconds=10):
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind((host, port))
    s.listen(1)
    print(f"Listening for connections on {host}:{port}")

    while True:
        conn, addr = s.accept()
        print(f"Connection from {addr}")

        try:
            while True:
                dummy_data = generate_dummy_data(chunk_size=5)
                chunk = pd.DataFrame(dummy_data)
                print(chunk)

                json_data = chunk.to_json(orient='records', lines=True)

                conn.send(json_data.encode('utf-8') + b'\n')

                time.sleep(interval_seconds)

        except (BrokenPipeError, ConnectionResetError, KeyboardInterrupt):
            print("Client disconnected.")
        finally:
            conn.close()
            print("Connection closed")
            
if __name__ == "__main__":
    send_data_over_socket(host='localhost', interval_seconds=10)
