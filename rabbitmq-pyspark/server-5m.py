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
    # Generate 5 dummy data for sensor lamps with unique IDs from 1 to 5
    unique_city_ids = random.sample(range(1, 6), min(chunk_size, 5))
    dummy_data = []

    for city_id in unique_city_ids:
        # Decide whether to generate anomaly for watt or not 
        generate_anomaly = random.uniform(0, 1) < anomaly_probability

        if generate_anomaly:
            # Generate anomaly data outside the specified range for watt
            anomaly_value_watt = random.choice([random.randint(200, 250), random.randint(300, 350)])
            watt_value = anomaly_value_watt
        else:
            # Generate normal data within the specified range for watt
            watt_value = random.randint(250, 300)

        # Generate normal data within the specified range for voltage (V)
        voltage_value = random.randint(100, 200)
        
        # Generate normal data within the specified range for current
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
    
# Import statements and function definitions remain unchanged
def send_data_over_socket(host='localhost', port=9999, interval_seconds=10):
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

                # Convert DataFrame to JSON string
                json_data = chunk.to_json(orient='records', lines=True)

                # Send JSON data over the socket
                conn.send(json_data.encode('utf-8') + b'\n')

                time.sleep(interval_seconds)

        except (BrokenPipeError, ConnectionResetError, KeyboardInterrupt):
            print("Client disconnected.")
        finally:
            conn.close()
            print("Connection closed")
            
if __name__ == "__main__":
    send_data_over_socket(host='localhost', interval_seconds=10)
