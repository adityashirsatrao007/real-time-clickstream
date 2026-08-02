"""Spark Structured Streaming clickstream analytics.

Consumes from Kafka topic "clicks", computes windowed aggregates, top pages,
and anomaly detection, and writes results to the console.

Run via:
    spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
        streaming/clickstream_analytics.py
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, window, count, countDistinct, avg, sum as _sum
)
from pyspark.sql.types import (
    StructType, StructField, StringType, TimestampType, DoubleType
)

KAFKA_BOOTSTRAP = "kafka:9092"
TOPIC = "clicks"

SCHEMA = StructType([
    StructField("event_id", StringType(), True),
    StructField("user_id", StringType(), True),
    StructField("session_id", StringType(), True),
    StructField("page", StringType(), True),
    StructField("locale", StringType(), True),
    StructField("event_type", StringType(), True),
    StructField("ts", TimestampType(), True),
])


def build_spark():
    return (
        SparkSession.builder
        .appName("clickstream-analytics")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )


def read_stream(spark):
    return (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", TOPIC)
        .option("startingOffsets", "latest")
        .load()
    )


def parse(raw):
    return raw.select(
        from_json(col("value").cast("string"), SCHEMA).alias("event")
    ).select("event.*").withColumn(
        "event_ts", col("ts").cast(TimestampType())
    ).withWatermark("event_ts", "30 seconds")


def main():
    spark = build_spark()
    raw = read_stream(spark)
    events = parse(raw)

    # Windowed session analytics (10s windows, 30s watermark)
    windowed = (
        events
        .groupBy(window(col("event_ts"), "10 seconds", "5 seconds"), col("page"))
        .agg(
            count("*").alias("events"),
            countDistinct("user_id").alias("unique_users"),
        )
        .orderBy(col("window").desc(), col("events").desc())
    )

    # Anomaly: users with unusually high event counts in a window
    per_user = (
        events
        .groupBy(window(col("event_ts"), "30 seconds"), col("user_id"))
        .agg(count("*").alias("user_events"))
        .filter(col("user_events") > 50)
    )

    # Write streaming aggregates to console for the demo.
    query1 = windowed.writeStream.outputMode("complete").format("console").start()
    query2 = per_user.writeStream.outputMode("complete").format("console").start()

    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
