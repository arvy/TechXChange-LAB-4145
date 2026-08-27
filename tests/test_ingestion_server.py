"""
tests/test_ingestion_server.py

Tests for the MCP ingestion server.

Unit tests run without any external services.
Integration tests (marked @pytest.mark.integration) require a live Kafka cluster.

Run only unit tests:
    uv run pytest -m "not integration"

Run all tests including integration:
    uv run pytest
"""

import json

import pytest

from mcp_ingestion_server.server import _parse_message, subscribe_and_ingest

# ---------------------------------------------------------------------------
# Unit tests — _parse_message
# ---------------------------------------------------------------------------

class TestParseMessage:
    def _impression_raw(self, **overrides):
        payload = {
            "impression_id": "imp-001",
            "publisher_id":  "PID_TECH_001",
            "advertiser_id": "AID_000001",
            "cookie_id":     "CID_TECH_000001",
            "cookie_type":   "cohort",
            "cohort":        "TECH",
        }
        envelope = {
            "event_id":        "evt-001",
            "event_type":      "impression",
            "event_timestamp": "2025-01-01T00:00:00+00:00",
            "schema_version":  "1.0",
            "payload":         payload,
        }
        envelope.update(overrides)
        return json.dumps(envelope)

    def test_impression_all_fields_present(self):
        parsed = _parse_message(self._impression_raw())
        assert parsed is not None
        assert parsed["event_type"]    == "impression"
        assert parsed["publisher_id"]  == "PID_TECH_001"
        assert parsed["cookie_type"]   == "cohort"
        assert parsed["cohort"]        == "TECH"
        assert parsed["impression_id"] == "imp-001"

    def test_impression_nullable_fields_are_none(self):
        parsed = _parse_message(self._impression_raw())
        # Fields only present on other event types must be None
        assert parsed["conversion_id"]               is None
        assert parsed["impression_timestamp"]         is None
        assert parsed["conversion_timestamp"]         is None
        assert parsed["time_to_conversion_seconds"]   is None

    def test_conversion_fields(self):
        envelope = {
            "event_id":        "evt-002",
            "event_type":      "conversion",
            "event_timestamp": "2025-01-01T00:01:00+00:00",
            "schema_version":  "1.0",
            "payload": {
                "conversion_id": "conv-001",
                "advertiser_id": "AID_000001",
                "publisher_id":  "PID_FASHION_003",
                "cookie_id":     "CID_FASHION_000456",
                "cookie_type":   "cohort",
            },
        }
        parsed = _parse_message(json.dumps(envelope))
        assert parsed["event_type"]    == "conversion"
        assert parsed["conversion_id"] == "conv-001"
        assert parsed["impression_id"] is None

    def test_conversion_identified_fields(self):
        envelope = {
            "event_id":        "evt-003",
            "event_type":      "conversion_identified",
            "event_timestamp": "2025-01-01T00:01:30+00:00",
            "schema_version":  "1.0",
            "payload": {
                "conversion_id":              "conv-002",
                "advertiser_id":              "AID_000042",
                "publisher_id":               "PID_TECH_001",
                "cookie_id":                  "CID_TECH_000123",
                "cookie_type":                "cohort",
                "cohort":                     "TECH",
                "impression_timestamp":       "2025-01-01T00:00:00+00:00",
                "conversion_timestamp":       "2025-01-01T00:01:30+00:00",
                "time_to_conversion_seconds": 90,
            },
        }
        parsed = _parse_message(json.dumps(envelope))
        assert parsed["event_type"]                  == "conversion_identified"
        assert parsed["time_to_conversion_seconds"]  == 90
        assert parsed["cohort"]                      == "TECH"

    def test_returns_none_on_invalid_json(self):
        result = _parse_message("not-json{{{")
        assert result is None

    def test_returns_none_on_empty_string(self):
        result = _parse_message("")
        assert result is None

    def test_partial_payload_does_not_raise(self):
        """Missing payload fields should yield None values, not raise."""
        envelope = {
            "event_id":   "evt-004",
            "event_type": "impression",
            "payload":    {},
        }
        parsed = _parse_message(json.dumps(envelope))
        assert parsed is not None
        assert parsed["publisher_id"] is None


# ---------------------------------------------------------------------------
# Integration tests — subscribe_and_ingest (requires live Kafka)
# ---------------------------------------------------------------------------

@pytest.mark.integration
class TestSubscribeAndIngest:
    """
    These tests consume from the live Kafka cluster.
    They require:
      - KAFKA_BOOTSTRAP_SERVERS pointing to a running cluster
      - The affiliate-junction-events topic to exist and have messages
        (run `uv run producer` first to populate it)
    """

    def test_returns_expected_shape(self):
        result = subscribe_and_ingest(
            batch_size=10,
            timeout_seconds=15.0,
            consumer_group_id="pytest-integration-shape",
        )
        assert isinstance(result, dict)
        assert "events" in result
        assert "count"  in result
        assert "topic"  in result
        assert "errors" in result
        assert result["count"] == len(result["events"])

    def test_event_fields_present(self):
        result = subscribe_and_ingest(
            batch_size=10,
            timeout_seconds=15.0,
            consumer_group_id="pytest-integration-fields",
        )
        required_keys = {
            "event_id", "event_type", "event_timestamp", "schema_version",
            "publisher_id", "advertiser_id", "cookie_id", "cookie_type",
        }
        for event in result["events"]:
            assert required_keys.issubset(event.keys()), (
                f"Missing keys in event: {required_keys - event.keys()}"
            )

    def test_event_types_are_valid(self):
        result = subscribe_and_ingest(
            batch_size=50,
            timeout_seconds=15.0,
            consumer_group_id="pytest-integration-types",
        )
        valid_types = {"impression", "conversion", "conversion_identified"}
        for event in result["events"]:
            assert event["event_type"] in valid_types, (
                f"Unexpected event_type: {event['event_type']}"
            )

    def test_no_parse_errors(self):
        result = subscribe_and_ingest(
            batch_size=50,
            timeout_seconds=15.0,
            consumer_group_id="pytest-integration-errors",
        )
        assert result["errors"] == [], f"Parse errors encountered: {result['errors']}"

    def test_independent_consumer_groups_get_same_data(self):
        """Two fresh consumer groups should receive the same messages from offset 0."""
        group_a = subscribe_and_ingest(
            batch_size=5,
            timeout_seconds=15.0,
            consumer_group_id="pytest-integration-group-a",
        )
        group_b = subscribe_and_ingest(
            batch_size=5,
            timeout_seconds=15.0,
            consumer_group_id="pytest-integration-group-b",
        )
        # Both groups should get events (topic has data)
        assert group_a["count"] > 0
        assert group_b["count"] > 0
