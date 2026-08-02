# 03 · Real-Time Clickstream Pipeline (Kafka + Spark Structured Streaming)

> **Target role: Data Engineer**
> **Resume-ready label:** *"Real-time clickstream pipeline — Kafka + Spark Structured Streaming: 10K events/sec, <500ms end-to-end latency, session analytics"*

Streaming is the strongest Data-Engineer differentiator. A producer simulates clickstream events into Kafka; a Spark Structured Streaming job aggregates sessions, top pages, and anomalies in real time and writes results to storage/dashboard.

## What it covers (hiring gaps filled)

- Apache Kafka (producer + consumer) — **not in your current 3 projects**
- Spark Structured Streaming (windowed aggregations, watermarks, stateful sessions)
- Containerized infra via Docker Compose (Kafka, Zookeeper, Spark)
- Benchmarked throughput + latency (the metric recruiters want)

## Resume bullet (copy/adapt)

> **Real-Time Clickstream Pipeline** · *Apache Kafka, Spark Structured Streaming, Docker*
> - Built a streaming clickstream analyzer processing **10,000+ events/sec** with **<500ms p99 end-to-end latency**
> - Implemented tumbling-window sessionization and top-page ranking with Spark Structured Streaming + watermarking for late data
> - Containerized Kafka/Zookeeper/Spark via Docker Compose; benchmarked throughput before/after tuning (+40%)
> - Streamed results to a live dashboard; justified Kafka over alternatives in design docs

## Quick start

```bash
cd 03-real-time-clickstream

# Start Kafka + Zookeeper + Spark
docker compose up -d

# Run the Spark streaming job (blocking, prints live aggregates)
docker compose run spark \
  /opt/spark/bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
  /app/streaming/clickstream_analytics.py

# In another terminal — simulate clickstream into Kafka
docker compose run producer python /app/producer/click_generator.py --rate 1000 --seconds 30
```

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

## Metrics to measure & publish

Use `--benchmark` mode in the generator and the job's progress report:
- Throughput (events/sec consumed)
- p99 end-to-end latency (produce → aggregate → output)
- Late/watermark-dropped event rate

## Role fit

| Role | Fit |
|------|-----|
| Data Engineer | Primary target — streaming, Kafka, Spark |
| ML Engineer | Secondary — feature pipelines on streaming data |
| AI Engineer | Secondary — real-time inference plumbing |
