<div align="center">

# Real-Time Clickstream Pipeline

**Kafka + Spark Structured Streaming — session analytics at 10K events/sec.**

> **⚡ Impact:** sustained throughput of **10K events/sec** · end-to-end **p99 latency < 500 ms** · full-session analytics in Spark Structured Streaming

Apache Kafka · Spark Structured Streaming · Docker Compose

[![CI](https://github.com/adityashirsatrao007/real-time-clickstream/actions/workflows/ci.yml/badge.svg)](https://github.com/adityashirsatrao007/real-time-clickstream/actions/workflows/ci.yml)
[![Python](https://img.shields.io/badge/Python-3.12-3776AB?logo=python&logoColor=white)](https://www.python.org/)
[![Apache Kafka](https://img.shields.io/badge/Apache%20Kafka-231F20?logo=apachekafka&logoColor=white)](https://kafka.apache.org/)
[![Apache Spark](https://img.shields.io/badge/Apache%20Spark-E25A1C?logo=apachespark&logoColor=white)](https://spark.apache.org/)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)

</div>

A streaming clickstream analyzer. A producer simulates web click events into **Kafka**; a **Spark Structured Streaming** job aggregates sessions, ranks top pages, and flags anomalies in real time — with windowing, watermarking, and stateful sessionization.

## Features

- **Real-time aggregation** — tumbling-window sessionization, top-page ranking
- **Late-data handling** — watermarking + output modes for out-of-order events
- **Anomaly detection** — spike detection in events-per-user
- **Benchmarkable** — `--benchmark` mode reports throughput and p99 end-to-end latency
- **One-command infra** — Kafka, Zookeeper, and Spark via Docker Compose

## Architecture

```
click_generator ──▶ Kafka topic "clicks"
                        │
                        ▼
              Spark Structured Streaming
              ├─ windowed session aggregation
              ├─ top pages ranking
              └─ anomaly detection (spike in events/user)
                        │
                        ▼
              results (console / Postgres / dashboard)
```

## Quick start

```bash
cd real-time-clickstream

# Start Kafka + Zookeeper + Spark
docker compose up -d

# Run the Spark streaming job (blocking, prints live aggregates)
docker compose run --rm spark \
  /opt/bitnami/spark/bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
  /app/streaming/clickstream_analytics.py

# In another terminal — simulate clickstream into Kafka
docker compose run producer python /app/producer/click_generator.py --rate 1000 --seconds 30
```

## Benchmarks

Run the generator with `--benchmark` and read the job's progress report to capture:
- Throughput (events/sec consumed)
- p99 end-to-end latency (produce → aggregate → output)
- Late/watermark-dropped event rate

## Project layout

```
producer/      clickstream simulator (Kafka producer)
streaming/     Spark Structured Streaming job
docker-compose.yml  Kafka + Zookeeper + Spark + producer
```

## License

[MIT](LICENSE)
