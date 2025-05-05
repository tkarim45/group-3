from kafka import KafkaProducer
import json
import random
import time
from datetime import datetime

producer = KafkaProducer(
    bootstrap_servers=["localhost:9092"],
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)

topic = "traffic_data"
sensor_ids = ["S1", "S2", "S3", "S4", "S5"]  # 5 sensors


def generate_traffic_data():
    return {
        "sensor_id": random.choice(sensor_ids),
        "timestamp": datetime.now().isoformat(),
        "vehicle_count": random.randint(0, 50),
        "average_speed": round(random.uniform(10, 100), 2),
        "congestion_level": random.choice(["LOW", "MEDIUM", "HIGH"]),
    }


while True:
    traffic_data = generate_traffic_data()
    producer.send(topic, traffic_data)
    print(f"Produced: {traffic_data}")
    time.sleep(1)
