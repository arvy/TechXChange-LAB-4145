#!/usr/bin/env python3
"""
Affiliate Junction Kafka Producer

Generates synthetic Affiliate Junction events (impression, conversion,
conversion_identified) and publishes them to a Confluent Platform topic.

Event schema is defined in the workshop README.
"""

import json
import logging
import os
import random
import signal
import time
import uuid
from collections import defaultdict, deque
from datetime import UTC, datetime, timedelta

from confluent_kafka import Producer
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Configuration (overridable via environment variables)
# ---------------------------------------------------------------------------

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC             = os.getenv("KAFKA_TOPIC", "affiliate-junction-events")
KAFKA_SECURITY_PROTOCOL = os.getenv("KAFKA_SECURITY_PROTOCOL", "PLAINTEXT")
KAFKA_SASL_MECHANISM    = os.getenv("KAFKA_SASL_MECHANISM", "")
KAFKA_SASL_USERNAME     = os.getenv("KAFKA_SASL_USERNAME", "")
KAFKA_SASL_PASSWORD     = os.getenv("KAFKA_SASL_PASSWORD", "")
KAFKA_SSL_CA_LOCATION   = os.getenv("KAFKA_SSL_CA_LOCATION", "")

ADVERTISERS_COUNT   = int(os.getenv("AFFILIATE_JUNCTION_ADVERTISERS_COUNT", "500"))
PUBLISHERS_COUNT    = int(os.getenv("AFFILIATE_JUNCTION_PUBLISHERS_COUNT", "1000"))
COOKIES_COUNT       = int(os.getenv("AFFILIATE_JUNCTION_COOKIES_COUNT", "5000"))
TRAFFIC_PER_CYCLE   = int(os.getenv("AFFILIATE_JUNCTION_TRAFFIC_MIN", "5000"))
SALES_PER_CYCLE     = int(os.getenv("AFFILIATE_JUNCTION_SALES_MIN", "500"))
HISTORY_MINS        = int(os.getenv("AFFILIATE_JUNCTION_HISTORY_MINS", "90"))
FRAUD_COOKIES_COUNT = int(os.getenv("AFFILIATE_JUNCTION_FRAUD_COOKIES_COUNT", "5"))
CYCLE_SECONDS       = int(os.getenv("AFFILIATE_JUNCTION_CYCLE_SECONDS", "60"))

COHORTS = os.getenv(
    "AFFILIATE_JUNCTION_COHORTS", "TECH,FASHION,HEALTH,FINANCE,TRAVEL"
).split(",")

COHORT_SAME_PROB       = float(os.getenv("AFFILIATE_JUNCTION_COHORT_SAME_PROBABILITY", "0.60"))
COHORT_DIFF_PROB       = float(os.getenv("AFFILIATE_JUNCTION_COHORT_DIFFERENT_PROBABILITY", "0.20"))
FRAUD_CROSS_PROB       = float(os.getenv("AFFILIATE_JUNCTION_FRAUD_CROSS_CONTAMINATION_PROBABILITY", "0.05"))
# Remaining probability (0.15) → random cookie

SCHEMA_VERSION = "1.0"


# ---------------------------------------------------------------------------
# Data pools
# ---------------------------------------------------------------------------

def build_data_pools():
    """Build advertisers, publishers, and cookie pools — mirrors generate_traffic.py."""
    advertisers = [f"AID_{i + 1:06d}" for i in range(ADVERTISERS_COUNT)]

    publishers_data = {}
    publishers = []
    publishers_per_cohort = PUBLISHERS_COUNT // len(COHORTS)
    remainder = PUBLISHERS_COUNT % len(COHORTS)

    for idx, cohort in enumerate(COHORTS):
        count = publishers_per_cohort + (1 if idx < remainder else 0)
        for i in range(count):
            pid = f"PID_{cohort}_{i + 1:03d}"
            publishers_data[pid] = {"id": pid, "cohort": cohort}
            publishers.append(pid)

    # Fraud cookies — each permanently tied to one publisher
    fraud_count = min(FRAUD_COOKIES_COUNT, len(publishers))
    fraud_publishers = random.sample(publishers, fraud_count)
    fraud_cookies = {
        f"CID_FRAUD_{i + 1:03d}": pub
        for i, pub in enumerate(fraud_publishers)
    }

    # Cohort cookies
    cohort_cookie_count = min(COOKIES_COUNT - len(fraud_cookies), len(publishers) * 2)
    cookies_per_publisher = max(1, cohort_cookie_count // len(publishers))
    cohort_cookies = {}
    counter = 1
    for pid in publishers:
        cohort = publishers_data[pid]["cohort"]
        for _ in range(cookies_per_publisher):
            if counter > cohort_cookie_count:
                break
            cohort_cookies[f"CID_{cohort}_{counter:06d}"] = cohort
            counter += 1

    # Random cookies
    remaining = COOKIES_COUNT - len(fraud_cookies) - len(cohort_cookies)
    random_cookies = [f"CID_RANDOM_{i + 1:06d}" for i in range(remaining)]

    all_cookies = list(fraud_cookies.keys()) + list(cohort_cookies.keys()) + random_cookies

    logger.info(
        "Data pools: %d advertisers, %d publishers (%d cohorts), "
        "%d fraud / %d cohort / %d random cookies",
        len(advertisers), len(publishers), len(COHORTS),
        len(fraud_cookies), len(cohort_cookies), len(random_cookies),
    )

    return advertisers, publishers, publishers_data, fraud_cookies, cohort_cookies, random_cookies, all_cookies


# ---------------------------------------------------------------------------
# Cookie selection (mirrors generate_traffic.py logic exactly)
# ---------------------------------------------------------------------------

def get_cookie_for_publisher(publisher_id, publishers_data, fraud_cookies, cohort_cookies, random_cookies, all_cookies):
    """Select a cookie for a publisher using the configured probability distribution."""
    # Fraud publisher → always return its fraud cookie
    for fraud_cookie, fraud_pub in fraud_cookies.items():
        if fraud_pub == publisher_id:
            return fraud_cookie, "fraud"

    publisher_cohort = publishers_data[publisher_id]["cohort"]
    rand = random.random()

    cumulative_same  = COHORT_SAME_PROB
    cumulative_diff  = COHORT_SAME_PROB + COHORT_DIFF_PROB
    cumulative_fraud = COHORT_SAME_PROB + COHORT_DIFF_PROB + FRAUD_CROSS_PROB

    if rand < cumulative_same:
        same = [c for c, cohort in cohort_cookies.items() if cohort == publisher_cohort]
        if same:
            return random.choice(same), "cohort"

    elif rand < cumulative_diff:
        diff = [c for c, cohort in cohort_cookies.items() if cohort != publisher_cohort]
        if diff:
            return random.choice(diff), "cohort"

    elif rand < cumulative_fraud:
        if fraud_cookies:
            cid = random.choice(list(fraud_cookies.keys()))
            return cid, "fraud"

    if random_cookies:
        return random.choice(random_cookies), "random"

    return random.choice(all_cookies), "random"


def derive_cohort(cookie_id, publishers_data, publisher_id):
    """Return the publisher's cohort (used on impression / conversion_identified events)."""
    return publishers_data.get(publisher_id, {}).get("cohort", "")


# ---------------------------------------------------------------------------
# Event builders
# ---------------------------------------------------------------------------

def make_envelope(event_type, payload):
    return {
        "event_id": str(uuid.uuid4()),
        "event_type": event_type,
        "event_timestamp": datetime.now(UTC).isoformat(),
        "schema_version": SCHEMA_VERSION,
        "payload": payload,
    }


def make_impression(publisher_id, advertiser_id, cookie_id, cookie_type, cohort):
    return make_envelope("impression", {
        "impression_id": str(uuid.uuid4()),
        "publisher_id":  publisher_id,
        "advertiser_id": advertiser_id,
        "cookie_id":     cookie_id,
        "cookie_type":   cookie_type,
        "cohort":        cohort,
    })


def make_conversion(advertiser_id, publisher_id, cookie_id, cookie_type):
    return make_envelope("conversion", {
        "conversion_id": str(uuid.uuid4()),
        "advertiser_id": advertiser_id,
        "publisher_id":  publisher_id,
        "cookie_id":     cookie_id,
        "cookie_type":   cookie_type,
    })


def make_conversion_identified(advertiser_id, publisher_id, cookie_id, cookie_type, cohort,
                                impression_ts, conversion_ts):
    delta = int((conversion_ts - impression_ts).total_seconds())
    return make_envelope("conversion_identified", {
        "conversion_id":               str(uuid.uuid4()),
        "advertiser_id":               advertiser_id,
        "publisher_id":                publisher_id,
        "cookie_id":                   cookie_id,
        "cookie_type":                 cookie_type,
        "cohort":                      cohort,
        "impression_timestamp":        impression_ts.isoformat(),
        "conversion_timestamp":        conversion_ts.isoformat(),
        "time_to_conversion_seconds":  delta,
    })


# ---------------------------------------------------------------------------
# Attribution tracker (90-minute sliding window, mirrors CookieImpressionTracker)
# ---------------------------------------------------------------------------

class AttributionTracker:
    def __init__(self, window_minutes=HISTORY_MINS):
        self.window = timedelta(minutes=window_minutes)
        self.cookie_impressions = defaultdict(deque)   # cookie_id → deque of (timestamp, publisher_id)

    def record(self, cookie_id, publisher_id, ts):
        self.cookie_impressions[cookie_id].append((ts, publisher_id))
        self._trim(cookie_id, ts)

    def get_impression(self, cookie_id, now):
        """Return the most recent (timestamp, publisher_id) for this cookie within the window, or None."""
        self._trim(cookie_id, now)
        impressions = self.cookie_impressions.get(cookie_id)
        if impressions:
            return impressions[-1]   # most recent
        return None

    def eligible(self, cookie_id, now):
        self._trim(cookie_id, now)
        return bool(self.cookie_impressions.get(cookie_id))

    def _trim(self, cookie_id, now):
        cutoff = now - self.window
        q = self.cookie_impressions[cookie_id]
        while q and q[0][0] < cutoff:
            q.popleft()
        if not q:
            del self.cookie_impressions[cookie_id]


# ---------------------------------------------------------------------------
# Kafka helpers
# ---------------------------------------------------------------------------

def build_producer_config():
    cfg = {"bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS}
    if KAFKA_SECURITY_PROTOCOL != "PLAINTEXT":
        cfg["security.protocol"] = KAFKA_SECURITY_PROTOCOL
    if KAFKA_SASL_MECHANISM:
        cfg["sasl.mechanism"] = KAFKA_SASL_MECHANISM
    if KAFKA_SASL_USERNAME:
        cfg["sasl.username"] = KAFKA_SASL_USERNAME
    if KAFKA_SASL_PASSWORD:
        cfg["sasl.password"] = KAFKA_SASL_PASSWORD
    if KAFKA_SSL_CA_LOCATION:
        cfg["ssl.ca.location"] = KAFKA_SSL_CA_LOCATION
    return cfg


def delivery_report(err, msg):
    if err:
        logger.warning("Delivery failed for %s: %s", msg.key(), err)


def publish(producer, event):
    """Serialise event to JSON and publish; use event_type as the Kafka message key."""
    producer.produce(
        topic=KAFKA_TOPIC,
        key=event["event_type"],
        value=json.dumps(event),
        callback=delivery_report,
    )


# ---------------------------------------------------------------------------
# Main loop
# ---------------------------------------------------------------------------

def run():
    logger.info("Starting Affiliate Junction Kafka Producer")
    logger.info("  Bootstrap servers : %s", KAFKA_BOOTSTRAP_SERVERS)
    logger.info("  Topic             : %s", KAFKA_TOPIC)
    logger.info("  Cycle             : %ds  (%d impressions, %d sales)",
                CYCLE_SECONDS, TRAFFIC_PER_CYCLE, SALES_PER_CYCLE)

    advertisers, publishers, publishers_data, fraud_cookies, cohort_cookies, random_cookies, all_cookies = (
        build_data_pools()
    )

    tracker = AttributionTracker()
    producer = Producer(build_producer_config())

    # Graceful shutdown
    running = True
    def _shutdown(sig, frame):
        nonlocal running
        logger.info("Shutdown signal received")
        running = False

    signal.signal(signal.SIGINT,  _shutdown)
    signal.signal(signal.SIGTERM, _shutdown)

    cycle = 0
    while running:
        cycle_start = time.time()
        cycle += 1
        now = datetime.now(UTC)

        impression_count = 0
        conversion_count = 0
        attributed_count = 0

        # --- Impressions ---
        for _ in range(TRAFFIC_PER_CYCLE):
            pub_id  = random.choice(publishers)
            adv_id  = random.choice(advertisers)
            cookie_id, cookie_type = get_cookie_for_publisher(
                pub_id, publishers_data, fraud_cookies, cohort_cookies, random_cookies, all_cookies
            )
            cohort = derive_cohort(cookie_id, publishers_data, pub_id)

            tracker.record(cookie_id, pub_id, now)

            event = make_impression(pub_id, adv_id, cookie_id, cookie_type, cohort)
            publish(producer, event)
            impression_count += 1

        # --- Conversions (with attribution) ---
        for _ in range(SALES_PER_CYCLE):
            adv_id  = random.choice(advertisers)
            pub_id  = random.choice(publishers)
            cookie_id, cookie_type = get_cookie_for_publisher(
                pub_id, publishers_data, fraud_cookies, cohort_cookies, random_cookies, all_cookies
            )

            # Always emit the raw conversion event
            event = make_conversion(adv_id, pub_id, cookie_id, cookie_type)
            publish(producer, event)
            conversion_count += 1

            # Emit conversion_identified only if cookie has a recent impression
            impression = tracker.get_impression(cookie_id, now)
            if impression:
                impression_ts, orig_pub_id = impression
                cohort = derive_cohort(cookie_id, publishers_data, orig_pub_id)
                event = make_conversion_identified(
                    adv_id, orig_pub_id, cookie_id, cookie_type, cohort,
                    impression_ts, now,
                )
                publish(producer, event)
                attributed_count += 1

        # Flush all pending messages
        producer.flush()

        elapsed = time.time() - cycle_start
        logger.info(
            "Cycle %d complete in %.2fs — %d impressions, %d conversions, %d attributed",
            cycle, elapsed, impression_count, conversion_count, attributed_count,
        )

        sleep_for = max(0.0, CYCLE_SECONDS - elapsed)
        if sleep_for > 0 and running:
            time.sleep(sleep_for)

    logger.info("Producer stopped after %d cycles", cycle)


if __name__ == "__main__":
    run()
