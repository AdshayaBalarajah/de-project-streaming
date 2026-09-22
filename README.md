# Real-Time Streaming Pipeline: Order Events

A real-time streaming data pipeline that generates simulated e-commerce order
events, streams them through a Kafka-compatible broker, and aggregates them
into rolling time windows using Apache Spark Structured Streaming.

Built to demonstrate the core skills that separate streaming data engineering
from batch ETL: event-driven ingestion, windowed aggregation, and watermarking
for handling late-arriving data.

## Architecture

Python event generator (producer.py)
↓
Redpanda (Kafka-compatible broker) — "orders" topic
↓
Spark Structured Streaming (spark_aggregator.py)

reads the stream
groups events into 10-second windows, per region
applies a watermark to tolerate late data
↓
Rolling aggregated totals, printed to console


- **Ingestion**: `producer.py` generates realistic fake order events
  (customer, product, amount, region) once per second using the `faker`
  library, and publishes them to Redpanda via `kafka-python`.
- **Broker**: [Redpanda](https://redpanda.com/) — a Kafka-API-compatible
  message broker, run locally via Docker Compose. Acts as the durable,
  ordered stream that producers write to and consumers read from.
- **Stream processing**: `spark_aggregator.py` uses Spark Structured
  Streaming to continuously read from the `orders` topic, group events into
  rolling 10-second windows per region, and compute running totals and order
  counts — refreshing as new data arrives.
- **Watermarking**: a 10-second watermark tells Spark how long to tolerate
  late-arriving events for a window before finalizing it, balancing result
  completeness against processing latency — a core concept in production
  streaming systems.
- Two lighter-weight scripts are also included for comparison:
  `consumer.py` (a plain Python consumer, no windowing) and `aggregator.py`
  (a hand-rolled Python windowing implementation, without watermarking) —
  useful for seeing how much Spark handles automatically that a manual
  implementation does not.

## Example output

+------------------------------------------+------+------------+-----------+
|window |region|total_amount|order_count|
+------------------------------------------+------+------------+-----------+
|{14:21:10, 14:21:20} |North |494.45 |1 |
|{14:21:10, 14:21:20} |East |419.71 |3 |
|{14:21:10, 14:21:20} |South |294.31 |2 |
+------------------------------------------+------+------------+-----------+


## Tech stack

- **Python** — `kafka-python-ng` (producing/consuming), `faker` (simulated data)
- **Redpanda** — Kafka-compatible streaming broker, run via Docker
- **Apache Spark (PySpark)** — Structured Streaming for windowed aggregation
- **Docker Compose** — one-command startup for the broker

## How to run it locally

Requires Docker Desktop and Python 3.

```bash
# 1. Start the broker
docker compose up -d
docker exec -it redpanda rpk topic create orders

# 2. Set up environment
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt

# 3. In one terminal: generate events
python3 producer.py

# 4. In another terminal: run the Spark windowed aggregation
python3 spark_aggregator.py
```

## What I'd change for a production version

- Sink aggregated results into a real table (e.g. a warehouse or Postgres)
  instead of printing to console, so results are queryable rather than
  ephemeral
- Add checkpointing so Spark can recover cleanly from a restart without
  reprocessing or losing data
- Run Redpanda with multiple partitions and Spark with parallel processing,
  for realistic throughput
- Add monitoring/alerting on consumer lag or processing delays

## What I learned

This project was built to understand the fundamental difference between
batch and streaming data processing — specifically why streaming systems
need windowing at all (a stream has no natural end to aggregate against),
and how watermarking provides a deliberate, tunable trade-off between
result completeness and latency, rather than assuming data always arrives
perfectly in order.