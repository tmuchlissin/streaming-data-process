import paho.mqtt.client as mqtt
import json
import random
import time
from datetime import datetime

# Define parameters
broker_address = "localhost"
topic = "sensor_data_topic"

# Create MQTT client
client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION1)

# Connect to broker
client.connect(broker_address)

# Define city-sensor ID mapping as integers
city_sensor_ids = {
    "Surabaya": [1, 2, 3],
    "Sidoarjo": [4, 5, 6]
}

def generate_sensor_data(city, current_timestamp):
    # Probability for anomalous data
    anomaly_probability = 0.05

    # Randomly select sensor_id
    sensor_id = random.choice(city_sensor_ids[city])

    if random.random() < anomaly_probability:
        # Generate anomalous watt value
        watt = random.choice((random.randint(0, 40), random.randint(70, 100)))
        i = round(random.uniform(0.2, 15), 2)  # Possibly adjust I for anomaly
    else:
        # Generate normal data outside the range (40-70)
        watt = random.randint(0, 100)
        while 40 <= watt <= 70:  # Keep generating until watt is outside the range
            watt = random.randint(0, 100)
        i = round(random.uniform(0.5, 10), 2)

    data = {
        "id_kWh": sensor_id,
        "city": city,
        "watt": watt,
        "I": i,
        "timestamp": current_timestamp
    }
    return data

try:
    while True:
        current_timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        # Randomly select a city for each iteration
        city = random.choice(list(city_sensor_ids.keys()))

        # Generate data for each id_kWh in the current city with the current timestamp
        data = generate_sensor_data(city, current_timestamp)

        if data is not None:
            # Convert data to JSON format
            json_data = json.dumps(data)

            # Publish JSON data
            client.publish(topic, json_data, retain=True)  # Set retain to True if desired

            # Print JSON data
            print(json_data)

        # Sleep for 1 second before the next record
        time.sleep(1)

except KeyboardInterrupt:
    client.disconnect()
