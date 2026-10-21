# Affiliate Junction — Kafka Producer

Generates synthetic Affiliate Junction events and publishes them to a
Confluent Platform topic. Implements the event schema defined in the
[workshop README](../README.md#event-schema).

## Quick start

```bash
# 1. Configure connection
cp env-sample .env
# Edit .env — at minimum set KAFKA_BOOTSTRAP_SERVERS

# 2. Run (dependencies are managed by uv at the repo root)
uv run producer
```

## What it produces

Three event types are published to the configured topic on every cycle:

| Event type | Volume per cycle | Description |
|---|---|---|
| `impression` | `AFFILIATE_JUNCTION_TRAFFIC_MIN` (default 5 000) | Ad served to a cookie |
| `conversion` | `AFFILIATE_JUNCTION_SALES_MIN` (default 500) | Purchase event |
| `conversion_identified` | Subset of conversions | Conversion attributed to a prior impression within the 90-min window |

Events use the `event_type` field as the Kafka message key, enabling
partition-level ordering per event type.

## Configuration

All settings are read from environment variables (or a `.env` file).
See [`env-sample`](env-sample) for the full reference.

Key variables:

| Variable | Default | Description |
|---|---|---|
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | Broker address |
| `KAFKA_TOPIC` | `affiliate-junction-events` | Target topic |
| `KAFKA_SECURITY_PROTOCOL` | `PLAINTEXT` | `PLAINTEXT` for local VM; `SASL_SSL` for Confluent Cloud |
| `AFFILIATE_JUNCTION_CYCLE_SECONDS` | `60` | Seconds between publish cycles |
| `AFFILIATE_JUNCTION_MAX_CYCLES` | `1` | Cycles to publish before exiting (~5 900 events each); `0` = run until interrupted |
| `AFFILIATE_JUNCTION_TRAFFIC_MIN` | `5000` | Impressions per cycle |
| `AFFILIATE_JUNCTION_SALES_MIN` | `500` | Conversions per cycle |

## Confluent Cloud vs Confluent Platform

**Confluent Platform (TechZone VM) — PLAINTEXT:**
```
KAFKA_BOOTSTRAP_SERVERS=<vm-ip>:9092
KAFKA_SECURITY_PROTOCOL=PLAINTEXT
```

**Confluent Cloud — SASL_SSL:**
```
KAFKA_BOOTSTRAP_SERVERS=<cluster>.confluent.cloud:9092
KAFKA_SECURITY_PROTOCOL=SASL_SSL
KAFKA_SASL_MECHANISM=PLAIN
KAFKA_SASL_USERNAME=<api-key>
KAFKA_SASL_PASSWORD=<api-secret>
```

## Stopping

By default the producer publishes `AFFILIATE_JUNCTION_MAX_CYCLES` cycles (one),
flushes, and exits. Set it to `0` to run continuously.

`Ctrl-C` (or `SIGTERM`) triggers a graceful shutdown at any time — the current
cycle finishes, pending messages are flushed, and the process exits cleanly.
