# cleanbots Kafka & Spark Streaming Interview Lab

Hands-on notes and examples for a Data Platform involving real-time robot telemetry.

## Architecture built

```text
Robot producers → Kafka topic → Bronze Delta → Silver Delta → Gold metrics
```

- **Kafka** transports events and retains partitioned logs.
- **Bronze** is immutable, replayable raw telemetry with Kafka metadata.
- **Silver** parses JSON, applies event-time rules, validates records, and deduplicates by `event_id`.
- **Gold** exposes trusted business metrics such as cleaned area, utilisation and downtime.

## What we covered

1. Topics, partitions, offsets, keys, brokers and replication
2. Consumer groups, offset commits, lag and rebalancing
3. Kafka delivery semantics: duplicates, idempotency, `acks=all`, ISR
4. Event time vs ingestion time, late data and watermarks
5. Bronze/Silver/Gold modelling with Delta and Structured Streaming
6. Cumulative telemetry counters and their correct aggregation
7. Hot partitions and key-design trade-offs

## Repository layout

- [`docs/kafka-core.md`](docs/kafka-core.md): concepts and interview answers
- [`docs/lionsbot-scenarios.md`](docs/lionsbot-scenarios.md): domain scenarios
- [`examples/bronze_to_delta.py`](examples/bronze_to_delta.py): Kafka to Bronze Delta
- [`examples/silver_from_bronze.py`](examples/silver_from_bronze.py): Bronze to Silver Delta
- [`docker/docker-compose.yml`](docker/docker-compose.yml): local Confluent Kafka + ZooKeeper

## Local learning setup

The Docker Compose configuration intentionally uses a single broker. It is ideal for partitioning, consumer groups, lag, offsets and Spark ingestion; it cannot demonstrate broker fault tolerance because replication factor must be one.

For Spark, use the Kafka connector and Delta package matching the Spark runtime, for example Spark 3.5.3:

```bash
--packages io.delta:delta-spark_2.12:3.2.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3
```

## Interview positioning

> Kafka is the transport; Bronze is the durable replayable record of what arrived; Silver applies schema, data quality, deduplication and event-time correctness; Gold provides governed metrics. In Databricks, I would generally implement this with Lakeflow pipelines, formerly DLT, while retaining explicit business idempotency and watermark decisions.
