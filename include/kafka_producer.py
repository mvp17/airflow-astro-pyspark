import time
import json
from datetime import datetime
import random
from kafka import KafkaProducer

KAFKA_BROKER = "kafka:9092"
TOPIC = "iot_data"

# Retry connecting to Kafka
while True:
    try:
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BROKER,
            value_serializer=lambda v: json.dumps(v).encode("utf-8")
        )
        print("✅ Connected to Kafka!")
        break  # Exit the loop once connected
    except Exception as e:
        print(f"❌ Kafka not available yet, retrying in 5 seconds... Error: {e}")
        time.sleep(5)

# Simulate sending IoT data
while True:
    device_id = random.randint(1, 10)
    temperature = random.uniform(20.0, 30.0)  # Random temperature between 20 and 30
    humidity = random.uniform(50.0, 80.0)  # Random humidity between 50 and 80
    location = {"lat": random.uniform(-90.0, 90.0), "lon": random.uniform(-180.0, 180.0)}  # Random lat/lon
    timestamp = str(datetime.now().strftime("%Y%m%d"))  # Current timestamp

    # Simulate IoT data
    data = {
        "device_id": device_id,
        "temperature": temperature,
        "humidity": humidity,
        "location": location,
        "timestamp": timestamp
    }

    # Send the data to Kafka
    producer.send(TOPIC, value=data)
    print(f"Sent: {data}")
    time.sleep(1)  # Send every second