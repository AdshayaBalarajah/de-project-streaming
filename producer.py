import json
import time
import random
from kafka import KafkaProducer
from faker import Faker

fake = Faker()

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

regions = ["North", "South", "East", "West"]
products = ["Laptop", "Headphones", "Coffee Mug", "Notebook", "Backpack", "Desk Lamp"]

print("Starting to produce order events... (Ctrl+C to stop)")

while True:
    order = {
        "order_id": fake.uuid4(),
        "customer_name": fake.name(),
        "product": random.choice(products),
        "amount": round(random.uniform(10, 500), 2),
        "region": random.choice(regions),
        "timestamp": time.time()
    }
    producer.send("orders", value=order)
    print(f"Sent: {order}")
    time.sleep(1)
    