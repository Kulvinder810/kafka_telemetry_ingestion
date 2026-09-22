# Kafka Core Concepts

## Topic, partition, key and offset

A topic is a named event stream. It is divided into partitions for parallelism. A Kafka **key** determines the partition; Kafka guarantees order only within that partition. An **offset** is a record's permanent sequential position within its partition.

For robot telemetry, `robot_id` is usually the key because battery, mission and cumulative readings require per-robot ordering.

## Consumer groups and lag

A group is one logical application. Multiple groups can each read every event. Within one group, one partition can be assigned to only one consumer.

```text
consumer_group + topic + partition → committed next offset
```

Lag is:

```text
log end offset − committed current offset
```

If there are three partitions and four consumers in one group, one consumer is idle.

## Reliable delivery

- `acks=0`: producer does not wait for a broker response.
- `acks=1`: leader acknowledges.
- `acks=all`: leader and required in-sync replicas acknowledge.

Production guidance for critical telemetry: replication factor 3, `acks=all`, and `min.insync.replicas=2`.

Kafka offsets identify Kafka records, not unique business events. A producer retry can create two records with different offsets for the same `event_id`; deduplicate in Silver.

## Event time and watermarks

Kafka arrival order is not necessarily business/event-time order. Use an event's `event_time` for windows and metrics.

With a 20-minute watermark:

```text
watermark = maximum event time observed − 20 minutes
```

Watermarks bound state for streaming deduplication and aggregations. They are not an SLA by themselves. The SLA also requires sufficient capacity, low consumer lag, monitoring, retry behaviour and reconciliation for very late events.

## Hot partitions

A hot partition occurs when one key dominates traffic. More consumers cannot split that partition within the same group.

Options:

1. Verify whether the producer is faulty or overly noisy.
2. Coalesce or throttle high-frequency telemetry at source.
3. Separate ordered lifecycle events from high-volume sensor readings.
4. Salt keys only when strict order for the entity is not required.
5. Isolate known heavy entities into a dedicated topic/traffic class.
6. Add partitions for better fleet-wide distribution, not as a fix for one hot ordered key.

## Cumulative counters

Never sum a cumulative odometer or cumulative area field. Calculate the delta between event-time-ordered readings with `LAG`; treat a decreasing counter as a reset or data-quality exception, not negative work.
