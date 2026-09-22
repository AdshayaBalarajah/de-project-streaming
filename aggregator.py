import json
import time
from collections import defaultdict
from kafka import KafkaConsumer

consumer = KafkaConsumer(
    "orders",
    bootstrap_servers='localhost:9092',
    auto_offset_reset='earliest',
    value_deserializer=lambda v: json.loads(v.decode('utf-8'))
)

WINDOW_SECONDS = 10

window_start = time.time()
totals = defaultdict(float)
counts = defaultdict(int)

print(f"Aggregating orders in {WINDOW_SECONDS}-second windows... (Ctrl+C to stop)")

for message in consumer:
    order = message.value
    region = order["region"]

    totals[region] += order["amount"]
    counts[region] += 1

    if time.time() - window_start >= WINDOW_SECONDS:
        print(f"\n--- Window closed at {time.strftime('%H:%M:%S')} ---")
        for r in totals:
            print(f"  {r}: {counts[r]} orders, ${totals[r]:.2f} total")

        window_start = time.time()
        totals = defaultdict(float)
        counts = defaultdict(int)
        