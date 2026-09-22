import json
from kafka import KafkaConsumer

consumer = KafkaConsumer(
    "orders",
    bootstrap_servers='localhost:9092',
    auto_offset_reset='earliest',
    value_deserializer=lambda v: json.loads(v.decode('utf-8'))
)

print("Listening for orders... (Ctrl+C to stop)")

for message in consumer:
    order = message.value
    print(f"Received: {order['product']} for ${order['amount']} in {order['region']}")