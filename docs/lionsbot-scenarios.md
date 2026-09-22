# LionsBot Interview Scenarios

## Late and duplicate telemetry

**Question:** A robot is offline and later sends 10:00, 10:05, 10:10, a duplicate 10:10, then a late 10:08 event. How do you keep metrics correct?

**Answer:** Persist all records unchanged with Kafka metadata in Bronze. In Silver, deduplicate by a producer-generated `event_id`, process using `event_time`, and accept late data within an agreed watermark/allowed-lateness policy. Recompute the affected metric window when allowed. Very late data goes to reconciliation, not silent loss. Commit source progress only after the durable write succeeds.

## Cumulative area

**Question:** Readings are 1,000, 1,025 and 1,050 m². What is the cleaned area during the period?

**Answer:** 50 m². Calculate each incremental difference (25 + 25), not `SUM(cumulative_area_m2)` which would incorrectly yield 3,075 m².

## First 90 days

- **0–30:** understand telemetry contract, consumers, current data quality and critical fleet metrics.
- **31–60:** stabilise raw ingestion, quality checks, observability, Silver model and a few trusted Gold metrics.
- **61–90:** semantic layer, governed self-service, anomaly detection, backfill/replay process and a prioritised platform roadmap.

## Strong summary

> I would key general robot lifecycle telemetry by `robot_id` for per-robot ordering, keep Kafka as transport, ingest it immutably to Bronze, apply event-time validation and idempotent deduplication in Silver, and expose governed metrics in Gold. I would monitor partition skew, consumer lag, freshness and quality failures as first-class operational metrics.
