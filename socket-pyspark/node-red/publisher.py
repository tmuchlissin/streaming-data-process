import paho.mqtt.client as mqtt
import json
import random
import time
from datetime import datetime, timedelta
import warnings

warnings.filterwarnings("ignore")

# Define parameters
broker_address = "localhost"
topic = "sensor_data_topic"

# Create MQTT client
client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION1)

# Connect to broker
client.connect(broker_address)

start_datetime = "2024-02-28 00:00:00"
current_timestamp = start_datetime

total_records_generated = 0
max_records = 100  # Set the maximum number of records
timestamp_increment = 0

# Define city-sensor ID mapping as integers
city_sensor_ids = {
    "Surabaya": [1, 2, 3],
    "Sidoarjo": [4, 5, 6]
}

def generate_sensor_data(city, sensor_id):
    # Probability for normal data
    normal_probability = 0.95

    # Probability for a reduced number of sensors (randomly selected from id_kWh 1-6)
    reduced_probability = 0.05  # Reinstated variable

    # Probability for anomalous data
    anomaly_probability = 0.05

    timestamp = current_timestamp  # Use the current timestamp

    if random.random() < normal_probability:
        if random.random() < anomaly_probability:
            # Generate anomalous watt value for normal case
            watt = random.choice((random.randint(0, 40), random.randint(70, 100)))
            i = round(random.uniform(0.2, 15), 2)  # Possibly adjust I for anomaly
        else:
            # Generate normal data
            watt = random.randint(40, 70)
            i = round(random.uniform(0.5, 10), 2)
    elif random.random() < reduced_probability:
        sensor_id = random.choice(city_sensor_ids[city])
        if random.random() < anomaly_probability:
            # Generate anomalous watt value for reduced case
            watt = random.choice((random.randint(0, 40), random.randint(70, 100)))
            i = round(random.uniform(0.2, 15), 2)  # Possibly adjust I for anomaly
        else:
            # Generate normal data
            watt = random.randint(40, 70)
            i = round(random.uniform(0.5, 10), 2)
    else:
        return None  # Do not generate data in this iteration

    data = {
        "id_kWh": sensor_id,
        "city": city,
        "watt": watt,
        "I": i,
        "timestamp": timestamp
    }
    return data

try:
    while total_records_generated < max_records:
        for city, sensor_ids in city_sensor_ids.items():
            for sensor_id in sensor_ids:
                # Generate data for each id_kWh in the current city with the current timestamp
                data = generate_sensor_data(city, sensor_id)

                if data is not None:
                    # Convert data to JSON format
                    json_data = json.dumps(data)

                    # Publish JSON data
                    client.publish(topic, json_data, retain=True)  # Set retain to True if desired

                    # Print JSON data
                    print(json_data)
                    total_records_generated += 10

        # Update timestamp for the next record
        timestamp_increment += 2  # Increase timestamp by 2 seconds
        current_timestamp = (datetime.strptime(start_datetime, "%Y-%m-%d %H:%M:%S") +
                            timedelta(seconds=timestamp_increment)).strftime("%Y-%m-%d %H:%M:%S")

        # Add 10 minutes to timestamp after every 10 records
        if total_records_generated % 11 == 0:
            timestamp_increment += 601  # Increase timestamp by 10 minutes

        # Sleep for 6 seconds before the next record
        time.sleep(6)

except KeyboardInterrupt:
    client.disconnect()   

