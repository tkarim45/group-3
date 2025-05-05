from kafka import KafkaConsumer
from prometheus_client import Gauge, Counter, start_http_server
import json
import threading

# Start Prometheus server on port 8001
start_http_server(8000)

# Define Prometheus metrics
volume_gauge = Gauge("traffic_volume", "Total vehicle count", ["sensor_id"])
congestion_counter = Counter(
    "traffic_congestion", "High congestion events", ["sensor_id"]
)
speed_gauge = Gauge("traffic_speed_avg", "Average speed", ["sensor_id"])
drops_gauge = Gauge("traffic_speed_drops", "Max speed during drops", ["sensor_id"])
busiest_gauge = Gauge("traffic_busiest", "Total vehicle count", ["sensor_id"])


# Function to consume from Kafka and update metrics
def consume_topic(topic, metric, value_field):
    consumer = KafkaConsumer(
        topic, bootstrap_servers="localhost:9092", auto_offset_reset="earliest"
    )
    for msg in consumer:
        print(f"Consumed message from {topic}: {msg.value}")
        data = json.loads(msg.value.decode("utf-8"))
        sensor_id = data["sensor_id"]
        value = data[value_field]  # Use the correct field name from streaming.py
        if isinstance(metric, Gauge):
            metric.labels(sensor_id=sensor_id).set(value)
        elif isinstance(metric, Counter):
            metric.labels(sensor_id=sensor_id).inc(value)


# Updated topics list to match streaming.py field names
topics = [
    ("volume_data", volume_gauge, "total_vehicle_count"),
    (
        "congestion_data",
        congestion_counter,
        "high_congestion_count",
    ),  # Changed to "high_congestion_count"
    ("speed_avg_data", speed_gauge, "average_speed"),  # Changed to "average_speed"
    ("speed_drops_data", drops_gauge, "max_speed"),
    ("busiest_sensors_data", busiest_gauge, "total_vehicle_count"),
]

threads = []
for topic, metric, field in topics:
    t = threading.Thread(target=consume_topic, args=(topic, metric, field))
    t.daemon = True
    t.start()
    threads.append(t)

# Keep main thread alive
try:
    while True:
        pass
except KeyboardInterrupt:
    print("Shutting down")
