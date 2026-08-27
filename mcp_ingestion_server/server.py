#!/usr/bin/env python3
"""
Affiliate Junction MCP Ingestion Server

Exposes Kafka → watsonx.data ingestion as MCP tools callable from
Claude Desktop or any MCP-compatible client.

Tools:
  subscribe_and_ingest  — consume a batch from the Affiliate Junction
                          Kafka topic and return structured events.
"""

import json
import logging
import os
import time

from confluent_kafka import Consumer, KafkaError, KafkaException
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
        # Start from the earliest unread offset for this group.
        # On first run (new group) this means the beginning of the topic.
        "auto.offset.reset":  "earliest",
        # Disable automatic offset commits — we commit only after
        # successfully processing the full batch.
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
    topic    = os.getenv("KAFKA_TOPIC", "affiliate-junction-events")
    cfg      = _build_consumer_config(consumer_group_id)

    logger.info(
        "subscribe_and_ingest: topic=%s group=%s batch=%d timeout=%.1fs",
        topic, consumer_group_id, batch_size, timeout_seconds,
    )

    consumer = Consumer(cfg)
    events: list[dict] = []
    errors: list[str]  = []

    try:
        consumer.subscribe([topic])

        # Poll until we have a full batch or time runs out
        deadline = time.monotonic() + timeout_seconds

        while len(events) < batch_size and time.monotonic() < deadline:
            remaining = max(0.0, deadline - time.monotonic())
            msg = consumer.poll(timeout=min(1.0, remaining))

            if msg is None:
                # No message within the 1s poll window — keep waiting
                continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    # Reached end of a partition — not an error
                    logger.debug("Partition EOF: %s [%d]", msg.topic(), msg.partition())
                    continue
                raise KafkaException(msg.error())

            parsed = _parse_message(msg.value().decode("utf-8"))
            if parsed:
                events.append(parsed)
            else:
                errors.append(f"partition={msg.partition()} offset={msg.offset()}")

        # Commit offsets only after the full batch is collected
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
# Entry point
# ---------------------------------------------------------------------------

def run():
    mcp.run()


if __name__ == "__main__":
    run()
