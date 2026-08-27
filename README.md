# Agentic AI Data Pipeline Workshop

> **TechXchange Lab LAB-4145** — Hands-On: Agentic Data Pipelines with MCP and watsonx.data

---

## Overview

This workshop demonstrates how the **Model Context Protocol (MCP)** can serve as a unifying layer across a full data lifecycle — from streaming ingestion through automated metadata governance to natural language-driven analysis — all grounded on watsonx.data's open lakehouse architecture.

Attendees build an end-to-end agentic data pipeline using a simulated **Affiliate Junction** event stream: a realistic affiliate marketing use-case with publishers, advertisers, cookies, cohorts, and fraud patterns.

```
Confluent Platform (Kafka)
        │  affiliate-junction-events topic
        ▼
  MCP Ingestion Server
  (subscribe_and_ingest tool)
        │  transform → typed schema
        ▼
  watsonx.data (Iceberg / Parquet)
  iceberg_data.affiliate_junction.kafka_events
        │
        ▼
  watsonx.data Intelligence
  (automated metadata cataloguing & governance)
        │
        ▼
  MCP Query Server
  (natural language → Presto SQL)
```

---

## Architecture

### Infrastructure (TechZone-provisioned)

| Resource | Type | Notes |
|---|---|---|
| Confluent Platform | Shared VM/VSI on IBM Cloud | Single Kafka broker; all attendees subscribe to the same topic |
| watsonx.data Premium | Shared SaaS instance | Attendees write to their own namespaced Iceberg tables |
| watsonx.data Intelligence | Shared SaaS instance | Automated cataloguing and governance |
| Attendee compute | Individual VSI (bx2-4x16) | Runs MCP servers and Jupyter notebooks |

### Lab Structure

| Exercise | Duration | What you build |
|---|---|---|
| **1 — Ingest** | ~25 min | MCP server that subscribes to the Kafka topic and lands events into your Iceberg table |
| **2 — Govern** | ~25 min | Connect watsonx.data Intelligence to your table; inspect auto-generated metadata |
| **3 — Query** | ~25 min | Wire up a natural language query interface via MCP backed by Presto |
| **Buffer** | ~15 min | Q&A, troubleshooting, stretch goals |

---

## Event Schema

The Kafka topic `affiliate-junction-events` carries three event types, all sharing a common envelope.

### Envelope (all events)

```json
{
  "event_id": "uuid-v4",
  "event_type": "impression | conversion | conversion_identified",
  "event_timestamp": "2025-07-14T10:23:45.123Z",
  "schema_version": "1.0",
  "payload": { }
}
```

---

### Event Type 1 — `impression`

Fired each time a publisher serves an ad to a cookie.

```json
{
  "event_id": "a1b2c3d4-e5f6-...",
  "event_type": "impression",
  "event_timestamp": "2025-07-14T10:23:45.123Z",
  "schema_version": "1.0",
  "payload": {
    "impression_id": "uuid-v4",
    "publisher_id": "PID_TECH_001",
    "advertiser_id": "AID_000042",
    "cookie_id": "CID_TECH_000123",
    "cookie_type": "cohort",
    "cohort": "TECH"
  }
}
```

| Field | Description |
|---|---|
| `impression_id` | Unique impression identifier (deduplication key) |
| `publisher_id` | Format: `PID_{COHORT}_{N}` |
| `advertiser_id` | Format: `AID_{NNNNNN}` |
| `cookie_id` | Format encodes type: `CID_FRAUD_*`, `CID_{COHORT}_*`, `CID_RANDOM_*` |
| `cookie_type` | Explicit classification: `cohort`, `fraud`, or `random` |
| `cohort` | Publisher cohort: `TECH`, `FASHION`, `HEALTH`, `FINANCE`, or `TRAVEL` |

---

### Event Type 2 — `conversion`

Fired when a purchase/conversion occurs against an advertiser.

```json
{
  "event_id": "b2c3d4e5-f6a7-...",
  "event_type": "conversion",
  "event_timestamp": "2025-07-14T10:24:10.456Z",
  "schema_version": "1.0",
  "payload": {
    "conversion_id": "uuid-v4",
    "advertiser_id": "AID_000042",
    "publisher_id": "PID_FASHION_003",
    "cookie_id": "CID_FASHION_000456",
    "cookie_type": "cohort"
  }
}
```

---

### Event Type 3 — `conversion_identified`

Fired when a conversion is successfully attributed back to an impression within the 90-minute attribution window. This is the analytically richest event — no join required downstream.

```json
{
  "event_id": "c3d4e5f6-a7b8-...",
  "event_type": "conversion_identified",
  "event_timestamp": "2025-07-14T10:24:10.456Z",
  "schema_version": "1.0",
  "payload": {
    "conversion_id": "uuid-v4",
    "advertiser_id": "AID_000042",
    "publisher_id": "PID_TECH_001",
    "cookie_id": "CID_TECH_000123",
    "cookie_type": "cohort",
    "cohort": "TECH",
    "impression_timestamp": "2025-07-14T10:21:30.000Z",
    "conversion_timestamp": "2025-07-14T10:24:10.456Z",
    "time_to_conversion_seconds": 160
  }
}
```

---

### Cookie Types & Cohorts

The Affiliate Junction demo simulates realistic traffic patterns:

| Cookie type | ID prefix | Description |
|---|---|---|
| `cohort` | `CID_{COHORT}_` | Normal user tied to a publisher cohort |
| `fraud` | `CID_FRAUD_` | Fraudulent cookie always associated with a specific publisher |
| `random` | `CID_RANDOM_` | Unattributed user with no cohort affiliation |

Cohorts: `TECH`, `FASHION`, `HEALTH`, `FINANCE`, `TRAVEL`

Traffic probabilities (configurable):
- **60%** — cohort cookie from the same cohort as publisher (legitimate attribution)
- **20%** — cohort cookie from a different cohort (cross-cohort browsing)
- **5%** — fraud cookie cross-contamination
- **15%** — random cookie

---

## Target Iceberg Table

The MCP ingestion server transforms the event envelope and lands all three event types into a single unified table (partitioned for efficient querying):

```sql
CREATE TABLE iceberg_data.affiliate_junction.kafka_events (
    event_id                    varchar,
    event_type                  varchar,
    event_timestamp             timestamp,
    schema_version              varchar,
    impression_id               varchar,
    conversion_id               varchar,
    publisher_id                varchar,
    advertiser_id               varchar,
    cookie_id                   varchar,
    cookie_type                 varchar,
    cohort                      varchar,
    impression_timestamp        timestamp,
    conversion_timestamp        timestamp,
    time_to_conversion_seconds  bigint,
    ingested_at                 timestamp
) WITH (
    format = 'PARQUET',
    partitioning = ARRAY['day(event_timestamp)', 'event_type']
);
```

Fields not applicable to a given event type (e.g. `impression_id` on a `conversion` event) are `NULL`.

---

## What This Adds vs the Existing Affiliate Junction Demo

| Aspect | Existing batch demo | This Kafka stream adds |
|---|---|---|
| Granularity | Aggregated impression counts per minute | Individual events — full drill-down capability |
| Cookie type | Encoded in ID string only | Explicit `cookie_type` field — directly queryable |
| Cohort | Encoded in publisher/cookie ID prefixes | Explicit `cohort` field on every event |
| Attribution | Derived in Iceberg via a join | `conversion_identified` emitted directly — no join needed |
| Latency | Batch (60s intervals) | Near real-time streaming |

---

## POC Story Cards

### Epic 1 — Affiliate Junction Kafka Producer

| # | Story |
|---|---|
| 1 | Design the Affiliate Junction event schema ✅ |
| 2 | Write a Python producer script that generates synthetic Affiliate Junction events and publishes to Confluent at a configurable rate ✅ |
| 3 | Validate producer script connects to Confluent Platform and publishes successfully ✅ |

### Epic 2 — MCP Ingestion Server

| # | Story |
|---|---|
| 4 | Stand up a local Python environment with the MCP SDK and Confluent Kafka client installed ✅ |
| 5 | Implement an MCP server with a single tool: `subscribe_and_ingest` — connects to the Confluent topic, consumes a batch of messages, returns them as structured data ✅ |
| 6 | Validate the MCP server tool is discoverable and callable from Claude Desktop |
| 7 | Add a transformation step: raw JSON events → flattened Iceberg-compatible schema (cast types, add `ingested_at` timestamp) |

### Epic 3 — watsonx.data Landing

| # | Story |
|---|---|
| 8 | Create the target `kafka_events` Iceberg table in the shared watsonx.data instance |
| 9 | Extend the MCP tool to write transformed events to the Iceberg table via the watsonx.data API or Presto JDBC |
| 10 | Validate end-to-end: producer publishes → MCP tool consumes & transforms → rows appear in Iceberg table |

### Epic 4 — Validation

| # | Story |
|---|---|
| 11 | Run a 10-minute soak test — producer running continuously, MCP tool called repeatedly — confirm no data loss, duplicates handled gracefully |
| 12 | Document architecture decisions (schema choices, batch size, auth approach) as the foundation for the lab guide |

---

## Repository Structure

```
agentic-ai-data-pipeline-workshop/
├── README.md                   # This file
├── run_tests.sh                # Test runner (unit + integration)
├── pytest.ini                  # pytest configuration
├── pyproject.toml              # uv project — dependencies and entry points
├── kafka-ca.crt                # TechZone Confluent Platform CA cert (gitignored)
├── producer/                   # Kafka producer (Stories 1–3)
├── mcp_ingestion_server/       # MCP ingestion server (Stories 4–7)
├── mcp-query-server/           # MCP query server / NL interface (Exercise 3)
├── schema/                     # Iceberg DDL and event schema definitions
├── tests/                      # Test suite
└── lab-guide/                  # Attendee-facing Jupyter notebooks
```

---

## Development

### Setup

```bash
# Install uv (if not already installed)
brew install uv

# Install dependencies and create virtualenv
uv sync

# Download the TechZone CA cert (replace <vm-ip> with your environment's IP)
scp -i cflt-vsi-key.pem root@<vm-ip>:/var/lib/confluent-access/kafka-ca.crt kafka-ca.crt

# Configure credentials
cp producer/env-sample producer/.env
cp mcp_ingestion_server/env-sample mcp_ingestion_server/.env
# Edit both .env files with your TechZone broker address and credentials
```

### Running

```bash
# Start the Kafka producer (publishes events continuously, Ctrl-C to stop)
uv run producer

# Start the MCP ingestion server (connect via Claude Desktop)
uv run mcp-ingestion-server
```

### Testing

```bash
# Unit tests only — no external services needed
./run_tests.sh

# Unit + integration tests — requires live Kafka with events in the topic
./run_tests.sh --all

# Lint only
./run_tests.sh --lint

# Lint + unit + integration
./run_tests.sh --full
```

---

## Prerequisites

- Python 3.11+
- `uv` ([install](https://docs.astral.sh/uv/getting-started/installation/))
- Access to the TechZone environment (credentials provided at lab start)
- Claude Desktop (for MCP tool validation in Story 6)
- No prior MCP experience required
