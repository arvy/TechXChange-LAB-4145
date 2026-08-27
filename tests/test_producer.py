"""
tests/test_producer.py

Unit tests for the Affiliate Junction Kafka producer.
No live Kafka connection required.
"""

import random
from datetime import UTC, datetime, timedelta

from producer.producer import (
    COHORTS,
    SCHEMA_VERSION,
    AttributionTracker,
    build_data_pools,
    derive_cohort,
    get_cookie_for_publisher,
    make_conversion,
    make_conversion_identified,
    make_impression,
)

# ---------------------------------------------------------------------------
# Data pool tests
# ---------------------------------------------------------------------------

class TestDataPools:
    def setup_method(self):
        self.ads, self.pubs, self.pubs_data, self.fraud, self.cohort, self.rand, self.all_c = (
            build_data_pools()
        )

    def test_advertiser_count(self):
        from producer.producer import ADVERTISERS_COUNT
        assert len(self.ads) == ADVERTISERS_COUNT

    def test_publisher_count(self):
        from producer.producer import PUBLISHERS_COUNT
        assert len(self.pubs) == PUBLISHERS_COUNT

    def test_publisher_ids_include_cohort(self):
        for pid in self.pubs:
            cohort = self.pubs_data[pid]["cohort"]
            assert cohort in COHORTS
            assert pid.startswith(f"PID_{cohort}_")

    def test_fraud_cookie_ids(self):
        for cid in self.fraud:
            assert cid.startswith("CID_FRAUD_")

    def test_cohort_cookie_ids(self):
        for cid, cohort in self.cohort.items():
            assert cohort in COHORTS
            assert cid.startswith(f"CID_{cohort}_")

    def test_random_cookie_ids(self):
        for cid in self.rand:
            assert cid.startswith("CID_RANDOM_")

    def test_total_cookie_pool(self):
        from producer.producer import COOKIES_COUNT
        assert len(self.all_c) == COOKIES_COUNT


# ---------------------------------------------------------------------------
# Cookie selection tests
# ---------------------------------------------------------------------------

class TestCookieSelection:
    def setup_method(self):
        self.ads, self.pubs, self.pubs_data, self.fraud, self.cohort, self.rand, self.all_c = (
            build_data_pools()
        )

    def test_fraud_publisher_always_returns_fraud_cookie(self):
        """A publisher with an assigned fraud cookie must always return it."""
        for fraud_cid, fraud_pub in self.fraud.items():
            cid, ctype = get_cookie_for_publisher(
                fraud_pub, self.pubs_data, self.fraud, self.cohort, self.rand, self.all_c
            )
            assert cid == fraud_cid
            assert ctype == "fraud"

    def test_cookie_type_valid(self):
        for _ in range(200):
            pub = random.choice(self.pubs)
            cid, ctype = get_cookie_for_publisher(
                pub, self.pubs_data, self.fraud, self.cohort, self.rand, self.all_c
            )
            assert ctype in ("cohort", "fraud", "random"), f"Unexpected cookie_type: {ctype}"
            assert cid in self.all_c

    def test_derive_cohort_returns_valid_cohort(self):
        pub = self.pubs[0]
        cohort = derive_cohort("irrelevant_cookie", self.pubs_data, pub)
        assert cohort in COHORTS


# ---------------------------------------------------------------------------
# Event builder tests
# ---------------------------------------------------------------------------

class TestEventBuilders:
    def setup_method(self):
        self.ads, self.pubs, self.pubs_data, self.fraud, self.cohort, self.rand, self.all_c = (
            build_data_pools()
        )

    def _sample(self):
        pub = random.choice(self.pubs)
        adv = random.choice(self.ads)
        cid, ctype = get_cookie_for_publisher(
            pub, self.pubs_data, self.fraud, self.cohort, self.rand, self.all_c
        )
        cohort = derive_cohort(cid, self.pubs_data, pub)
        return pub, adv, cid, ctype, cohort

    def test_impression_envelope(self):
        pub, adv, cid, ctype, cohort = self._sample()
        event = make_impression(pub, adv, cid, ctype, cohort)
        assert event["event_type"] == "impression"
        assert event["schema_version"] == SCHEMA_VERSION
        assert set(event["payload"].keys()) == {
            "impression_id", "publisher_id", "advertiser_id",
            "cookie_id", "cookie_type", "cohort",
        }
        assert event["payload"]["publisher_id"] == pub
        assert event["payload"]["cookie_type"] == ctype

    def test_conversion_envelope(self):
        pub, adv, cid, ctype, _ = self._sample()
        event = make_conversion(adv, pub, cid, ctype)
        assert event["event_type"] == "conversion"
        assert set(event["payload"].keys()) == {
            "conversion_id", "advertiser_id", "publisher_id", "cookie_id", "cookie_type",
        }

    def test_conversion_identified_envelope(self):
        pub, adv, cid, ctype, cohort = self._sample()
        now = datetime.now(UTC)
        impression_ts = now - timedelta(seconds=90)
        event = make_conversion_identified(adv, pub, cid, ctype, cohort, impression_ts, now)
        assert event["event_type"] == "conversion_identified"
        payload = event["payload"]
        assert set(payload.keys()) == {
            "conversion_id", "advertiser_id", "publisher_id", "cookie_id",
            "cookie_type", "cohort", "impression_timestamp", "conversion_timestamp",
            "time_to_conversion_seconds",
        }
        assert payload["time_to_conversion_seconds"] == 90
        assert payload["cohort"] == cohort

    def test_event_ids_are_unique(self):
        pub, adv, cid, ctype, cohort = self._sample()
        events = [make_impression(pub, adv, cid, ctype, cohort) for _ in range(100)]
        ids = [e["event_id"] for e in events]
        assert len(set(ids)) == 100  # all unique


# ---------------------------------------------------------------------------
# Attribution tracker tests
# ---------------------------------------------------------------------------

class TestAttributionTracker:
    def test_records_and_retrieves(self):
        tracker = AttributionTracker()
        now = datetime.now(UTC)
        tracker.record("cookie-1", "pub-1", now)
        result = tracker.get_impression("cookie-1", now)
        assert result is not None
        assert result[1] == "pub-1"

    def test_window_expiry(self):
        from producer.producer import HISTORY_MINS
        tracker = AttributionTracker()
        old_ts = datetime.now(UTC) - timedelta(minutes=HISTORY_MINS + 1)
        tracker.record("cookie-2", "pub-2", old_ts)
        # Check against current time — impression is outside the window
        result = tracker.get_impression("cookie-2", datetime.now(UTC))
        assert result is None

    def test_most_recent_impression_returned(self):
        tracker = AttributionTracker()
        now = datetime.now(UTC)
        tracker.record("cookie-3", "pub-a", now - timedelta(minutes=5))
        tracker.record("cookie-3", "pub-b", now - timedelta(minutes=1))
        result = tracker.get_impression("cookie-3", now)
        assert result[1] == "pub-b"

    def test_eligible_check(self):
        tracker = AttributionTracker()
        now = datetime.now(UTC)
        assert not tracker.eligible("cookie-x", now)
        tracker.record("cookie-x", "pub-x", now)
        assert tracker.eligible("cookie-x", now)
