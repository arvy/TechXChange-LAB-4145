#!/usr/bin/env python3
"""
Affiliate Junction MCP Ingestion Server

Exposes Kafka → watsonx.data ingestion as MCP tools callable from
Claude Desktop or any MCP-compatible client.

Tools:
  subscribe_and_ingest  — consume a batch from the Affiliate Junction
                          Kafka topic and return structured events.
  ingest_to_iceberg     — consume a batch from Kafka and write rows
                          directly into the watsonx.data Iceberg table.
"""

import json
import logging
import os
import time

import requests
from confluent_kafka import Consumer, KafkaError, KafkaException, TopicPartition
from dotenv import load_dotenv
from mcp.server.fastmcp import FastMCP

# Resolve paths relative to the project root (one level up from this file).
_HERE        = os.path.dirname(os.path.abspath(__file__))
_PROJECT_ROOT = os.path.dirname(_HERE)
load_dotenv(os.path.join(_HERE, ".env"))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# MCP server
# ---------------------------------------------------------------------------

mcp = FastMCP("affiliate-junction-ingestion")

# ---------------------------------------------------------------------------
# Kafka helpers
# ---------------------------------------------------------------------------

def _build_consumer_config(group_id: str) -> dict:
    cfg = {
        "bootstrap.servers":  os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
        "group.id":           group_id,
        # Used only for offset storage; actual partition assignment is manual
        # via consumer.assign() so all partitions are polled on every call.
        "auto.offset.reset":  "earliest",
        "enable.auto.commit": False,
    }
    protocol = os.getenv("KAFKA_SECURITY_PROTOCOL", "PLAINTEXT")
    if protocol != "PLAINTEXT":
        cfg["security.protocol"] = protocol
    mechanism = os.getenv("KAFKA_SASL_MECHANISM", "")
    if mechanism:
        cfg["sasl.mechanism"]  = mechanism
        cfg["sasl.username"]   = os.getenv("KAFKA_SASL_USERNAME", "")
        cfg["sasl.password"]   = os.getenv("KAFKA_SASL_PASSWORD", "")
    ca = os.getenv("KAFKA_SSL_CA_LOCATION", "")
    if ca:
        # Resolve relative paths against the project root
        cfg["ssl.ca.location"] = ca if os.path.isabs(ca) else os.path.join(_PROJECT_ROOT, ca)
    return cfg


def _parse_message(raw: str) -> dict | None:
    """Parse a raw Kafka message value into a flat event dict, or None on error."""
    try:
        envelope = json.loads(raw)
        payload  = envelope.get("payload", {})
        return {
            "event_id":                    envelope.get("event_id"),
            "event_type":                  envelope.get("event_type"),
            "event_timestamp":             envelope.get("event_timestamp"),
            "schema_version":              envelope.get("schema_version"),
            # impression fields
            "impression_id":               payload.get("impression_id"),
            # conversion fields
            "conversion_id":               payload.get("conversion_id"),
            # shared identity fields
            "publisher_id":                payload.get("publisher_id"),
            "advertiser_id":               payload.get("advertiser_id"),
            "cookie_id":                   payload.get("cookie_id"),
            "cookie_type":                 payload.get("cookie_type"),
            "cohort":                      payload.get("cohort"),
            # conversion_identified fields
            "impression_timestamp":        payload.get("impression_timestamp"),
            "conversion_timestamp":        payload.get("conversion_timestamp"),
            "time_to_conversion_seconds":  payload.get("time_to_conversion_seconds"),
        }
    except (json.JSONDecodeError, AttributeError) as exc:
        logger.warning("Failed to parse message: %s — %s", raw[:120], exc)
        return None


# ---------------------------------------------------------------------------
# MCP tool
# ---------------------------------------------------------------------------

@mcp.tool()
def subscribe_and_ingest(
    batch_size: int = 100,
    timeout_seconds: float = 10.0,
    consumer_group_id: str = "mcp-ingestion-default",
) -> dict:
    """Consume a batch of Affiliate Junction events from Kafka.

    Connects to the configured Kafka topic, polls up to `batch_size` messages
    within `timeout_seconds`, and returns them as a structured list ready for
    downstream transformation and loading into watsonx.data.

    Args:
        batch_size: Maximum number of messages to consume (default 100).
        timeout_seconds: Maximum wall-clock time to wait for messages (default 10s).
        consumer_group_id: Kafka consumer group ID. Use a unique value per
            attendee to maintain independent offsets (default
            "mcp-ingestion-default").

    Returns:
        A dict with:
          - events: list of parsed event dicts
          - count: number of events returned
          - topic: the Kafka topic consumed from
          - consumer_group_id: the group ID used
          - errors: list of any per-message parse errors encountered
    """
    topic        = os.getenv("KAFKA_TOPIC", "affiliate-junction-events")
    num_partitions = int(os.getenv("KAFKA_TOPIC_PARTITIONS", "3"))
    cfg          = _build_consumer_config(consumer_group_id)

    logger.info(
        "subscribe_and_ingest: topic=%s group=%s batch=%d timeout=%.1fs",
        topic, consumer_group_id, batch_size, timeout_seconds,
    )

    consumer = Consumer(cfg)
    events: list[dict] = []
    errors: list[str]  = []

    try:
        # Assign all partitions explicitly at their committed offsets so that
        # the group coordinator cannot serialise assignment (which would cause
        # one partition to be drained before the next is assigned).
        committed = consumer.committed(
            [TopicPartition(topic, p) for p in range(num_partitions)]
        )
        partitions = [
            TopicPartition(topic, tp.partition, max(tp.offset, 0))
            for tp in committed
        ]
        consumer.assign(partitions)

        # Seek each partition to its starting offset so the internal fetch
        # buffers are primed from the correct position on all partitions
        # before we start consuming.
        for tp in partitions:
            consumer.seek(tp)

        # Round-robin across partitions: collect up to per_partition messages
        # from each partition in turn until the batch is full or time runs out.
        # This guarantees a proportional mix of event types regardless of how
        # many messages each partition holds.
        per_partition = max(1, batch_size // num_partitions)
        deadline      = time.monotonic() + timeout_seconds

        for tp in partitions:
            if len(events) >= batch_size or time.monotonic() >= deadline:
                break
            collected = 0
            while collected < per_partition and time.monotonic() < deadline:
                remaining = max(0.0, deadline - time.monotonic())
                msg = consumer.poll(timeout=min(1.0, remaining))
                if msg is None:
                    break  # no more messages on this partition right now
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        logger.debug("Partition EOF: %s [%d]", msg.topic(), msg.partition())
                        break
                    raise KafkaException(msg.error())
                if msg.partition() != tp.partition:
                    continue  # stale pre-fetch from wrong partition; discard
                parsed = _parse_message(msg.value().decode("utf-8"))
                if parsed:
                    events.append(parsed)
                    collected += 1
                else:
                    errors.append(f"partition={msg.partition()} offset={msg.offset()}")

        # Fill any remaining slots (e.g. a partition had fewer than per_partition
        # messages) from wherever messages are still available.
        while len(events) < batch_size and time.monotonic() < deadline:
            remaining = max(0.0, deadline - time.monotonic())
            msg = consumer.poll(timeout=min(1.0, remaining))
            if msg is None:
                break
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                raise KafkaException(msg.error())
            parsed = _parse_message(msg.value().decode("utf-8"))
            if parsed:
                events.append(parsed)
            else:
                errors.append(f"partition={msg.partition()} offset={msg.offset()}")

        # Commit offsets only if we actually consumed something
        if events:
            consumer.commit(asynchronous=False)

        logger.info(
            "subscribe_and_ingest complete: %d events, %d errors",
            len(events), len(errors),
        )

    finally:
        consumer.close()

    return {
        "events":            events,
        "count":             len(events),
        "topic":             topic,
        "consumer_group_id": consumer_group_id,
        "errors":            errors,
    }


# ---------------------------------------------------------------------------
# watsonx.data helpers
# ---------------------------------------------------------------------------

def _get_iam_token() -> str:
    """Exchange an IBM Cloud API key for a short-lived IAM bearer token."""
    api_key = os.getenv("WATSONX_DATA_API_KEY", "")
    resp = requests.post(
        "https://iam.cloud.ibm.com/identity/token",
        data={
            "grant_type": "urn:ibm:params:oauth:grant-type:apikey",
            "apikey": api_key,
        },
        headers={"Content-Type": "application/x-www-form-urlencoded"},
        timeout=30,
    )
    resp.raise_for_status()
    return resp.json()["access_token"]


def _run_sql_job(sql: str, engine_id: str, token: str) -> dict:
    """Submit a SQL statement to watsonx.data via the Presto gateway and poll until complete.

    Uses the /v3/v1/statement endpoint (Presto HTTP API proxy) which is available
    on all watsonx.data SaaS instances regardless of version.
    """
    base_url    = os.getenv("WATSONX_DATA_BASE_URL", "").rstrip("/")
    instance_id = os.getenv("WATSONX_DATA_INSTANCE_ID", "")

    headers = {
        "Authorization":  f"Bearer {token}",
        "AuthInstanceId": instance_id,
        "Content-Type":   "application/json",
        "Accept":         "application/json",
    }

    # Derive catalog from target table (first component of fully-qualified name)
    catalog = os.getenv("WATSONX_DATA_TARGET_TABLE", "my_data.affiliate_junction.kafka_events").split(".")[0]
    schema  = os.getenv("WATSONX_DATA_TARGET_TABLE", "my_data.affiliate_junction.kafka_events").split(".")[1]

    path = f"{base_url}/v3/v1/statement?engine_id={engine_id}"

    # Submit
    resp = requests.post(
        path,
        headers=headers,
        json={
            "source":            "mcp-ingestion-server",
            "catalog":           catalog,
            "schema":            schema,
            "sqlQuery":          sql,
            "prepared_statement": "",
            "session":           "",
        },
        timeout=30,
    )
    resp.raise_for_status()
    data     = resp.json().get("data", {}) or {}
    query_id = data.get("id")
    next_uri = data.get("nextUri", "")
    state    = (data.get("stats") or {}).get("state", "")

    logger.info("_run_sql_job: submitted query_id=%s initial_state=%s", query_id, state)

    # Poll until FINISHED / FAILED / CANCELED
    for _ in range(120):
        if state in ("FINISHED", "FAILED", "CANCELED"):
            break
        time.sleep(1)
        poll_resp = requests.post(
            path,
            headers=headers,
            json={
                "source":  "mcp-ingestion-server",
                "catalog": catalog,
                "schema":  schema,
                "nextUri": next_uri,
            },
            timeout=15,
        )
        poll_resp.raise_for_status()
        data     = poll_resp.json().get("data", {}) or {}
        state    = (data.get("stats") or {}).get("state", "")
        next_uri = data.get("nextUri", next_uri)
    else:
        return {"status": "timeout", "query_id": query_id}

    if state == "FINISHED":
        return {"status": "completed", "query_id": query_id, "rows": data.get("updateCount", 0)}
    return {"status": "failed", "query_id": query_id, "state": state}


def _events_to_insert_sql(events: list[dict], table: str) -> str:
    """Build a single INSERT … VALUES statement for a list of event dicts."""

    def _sql_str(v) -> str:
        """Render a Python value as a Presto SQL literal."""
        if v is None:
            return "NULL"
        if isinstance(v, (int, float)):
            return str(v)
        # Escape single quotes and wrap in quotes
        escaped = str(v).replace("'", "''")
        return f"'{escaped}'"

    def _ts(v) -> str:
        """Render an ISO timestamp string as a Presto TIMESTAMP literal."""
        if v is None:
            return "NULL"
        # Strip timezone suffix — Presto TIMESTAMP is timezone-naive
        ts = str(v).replace("+00:00", "").replace("Z", "").replace("T", " ")
        return f"TIMESTAMP '{ts}'"

    now_ts = _ts(time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime()))

    rows = []
    for e in events:
        row = ", ".join([
            _sql_str(e.get("event_id")),
            _sql_str(e.get("event_type")),
            _ts(e.get("event_timestamp")),
            _sql_str(e.get("schema_version")),
            _sql_str(e.get("impression_id")),
            _sql_str(e.get("conversion_id")),
            _sql_str(e.get("publisher_id")),
            _sql_str(e.get("advertiser_id")),
            _sql_str(e.get("cookie_id")),
            _sql_str(e.get("cookie_type")),
            _sql_str(e.get("cohort")),
            _ts(e.get("impression_timestamp")),
            _ts(e.get("conversion_timestamp")),
            _sql_str(e.get("time_to_conversion_seconds")),
            now_ts,   # ingested_at
        ])
        rows.append(f"({row})")

    values = ",\n  ".join(rows)
    return f"INSERT INTO {table} VALUES\n  {values}"


# ---------------------------------------------------------------------------
# MCP tool — ingest_to_iceberg
# ---------------------------------------------------------------------------

@mcp.tool()
def ingest_to_iceberg(
    batch_size: int = 100,
    timeout_seconds: float = 10.0,
    consumer_group_id: str = "mcp-iceberg-default",
    target_table: str | None = None,
    engine_id: str | None = None,
) -> dict:
    """Consume a batch of Affiliate Junction events from Kafka and write them
    to a watsonx.data Iceberg table.

    This tool combines subscribe_and_ingest with a Presto INSERT, landing
    events directly into the Iceberg table in a single call.

    Args:
        batch_size: Maximum number of messages to consume (default 100).
        timeout_seconds: Maximum wall-clock time to wait for messages (default 10s).
        consumer_group_id: Kafka consumer group ID — use a unique value per
            attendee to maintain independent offsets (default
            "mcp-iceberg-default").
        target_table: Fully-qualified Iceberg table name
            (default "my_data.affiliate_junction.kafka_events").
        engine_id: watsonx.data Presto engine to use (default "presto44").

    Returns:
        A dict with:
          - rows_written: number of rows inserted
          - kafka_count: number of events consumed from Kafka
          - kafka_errors: parse errors encountered during consumption
          - job_status: watsonx.data SQL job status
          - target_table: the table written to
    """
    # Resolve defaults from env so attendees only need to configure .env
    if target_table is None:
        target_table = os.getenv(
            "WATSONX_DATA_TARGET_TABLE", "my_data.affiliate_junction.kafka_events"
        )
    if engine_id is None:
        engine_id = os.getenv("WATSONX_DATA_ENGINE_ID", "presto44")

    # Step 1 — consume from Kafka
    kafka_result = subscribe_and_ingest(
        batch_size=batch_size,
        timeout_seconds=timeout_seconds,
        consumer_group_id=consumer_group_id,
    )
    events = kafka_result["events"]

    if not events:
        logger.info("ingest_to_iceberg: no events consumed, skipping INSERT")
        return {
            "rows_written":  0,
            "kafka_count":   0,
            "kafka_errors":  kafka_result["errors"],
            "job_status":    "skipped",
            "target_table":  target_table,
        }

    # Step 2 — build INSERT SQL
    sql = _events_to_insert_sql(events, target_table)
    logger.info(
        "ingest_to_iceberg: inserting %d rows into %s", len(events), target_table
    )

    # Step 3 — get IAM token and run the SQL job
    token  = _get_iam_token()
    job    = _run_sql_job(sql, engine_id, token)
    status = job.get("status", "unknown")

    logger.info("ingest_to_iceberg: job %s — status=%s", job.get("job_id"), status)

    return {
        "rows_written":  len(events) if status == "completed" else 0,
        "kafka_count":   kafka_result["count"],
        "kafka_errors":  kafka_result["errors"],
        "job_status":    status,
        "target_table":  target_table,
    }


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def run():
    mcp.run()


if __name__ == "__main__":
    run()
