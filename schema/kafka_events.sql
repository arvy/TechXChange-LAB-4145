-- =============================================================================
-- Affiliate Junction — your personal SQL for the watsonx.data Query Workspace
-- Run the statements in the Presto engine, one at a time.
--
-- Part 1  Set up your schema and table   (section 5.1, run now, in order)
-- Part 2  Check for duplicate rows       (section 5.5, only after loading)
-- Part 3  Verify the loaded data         (section 5.6)
-- Part 4  Analytical queries             (section 7.2)
-- Part 5  More queries and a table check (section 10 and the reference section)
-- =============================================================================


-- =============================================================================
-- Part 1 — Set up your schema and table
-- =============================================================================

-- Step 1: Create your personal schema (namespaces your tables per attendee).
-- Replace <your-attendee-id> with your assigned ID (e.g. student1).
-- Replace <cos-bucket-name> with the COS bucket name (Infrastructure manager).
-- Or fill both in for you: uv run python schema/generate_sql.py

CREATE SCHEMA IF NOT EXISTS demo_data.affiliate_junction_<your-attendee-id>
WITH (location = 's3a://<cos-bucket-name>/affiliate_junction_<your-attendee-id>/');


-- Step 2: Create the kafka_events landing table.
-- All three event types (impression, conversion, conversion_identified) land here.
-- Fields not applicable to a given event type are NULL.

CREATE TABLE IF NOT EXISTS demo_data.affiliate_junction_<your-attendee-id>.kafka_events (
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


-- Step 3: Verify the table was created (returns no rows until you load events)
SELECT * FROM demo_data.affiliate_junction_<your-attendee-id>.kafka_events LIMIT 5;


-- =============================================================================
-- Part 2 — Check for duplicate rows (section 5.5, after a call timed out)
-- =============================================================================

-- If total is greater than distinct_events, you have duplicates.
SELECT count(*) AS total, count(DISTINCT event_id) AS distinct_events
FROM demo_data.affiliate_junction_<your-attendee-id>.kafka_events;

-- Only if you have duplicates: delete all rows, then reload with a new
-- consumer group. Remove the leading "-- " to run it.
-- DELETE FROM demo_data.affiliate_junction_<your-attendee-id>.kafka_events;


-- =============================================================================
-- Part 3 — Verify the loaded data (section 5.6)
-- =============================================================================

-- How many rows landed?
SELECT count(*) AS row_count
FROM demo_data.affiliate_junction_<your-attendee-id>.kafka_events;

-- What event types do we have?
SELECT event_type, count(*) AS cnt
FROM demo_data.affiliate_junction_<your-attendee-id>.kafka_events
GROUP BY event_type
ORDER BY cnt DESC;

-- Show 5 sample rows
SELECT event_id, event_type, publisher_id, cookie_type, cohort, ingested_at
FROM demo_data.affiliate_junction_<your-attendee-id>.kafka_events
LIMIT 5;


-- =============================================================================
-- Part 4 — Analytical queries (section 7.2)
-- =============================================================================

-- Average time to convert, by publisher cohort
SELECT
    cohort,
    ROUND(AVG(time_to_conversion_seconds), 1) AS avg_seconds,
    COUNT(*) AS conversions
FROM demo_data.affiliate_junction_<your-attendee-id>.kafka_events
WHERE event_type = 'conversion_identified'
  AND cohort IS NOT NULL
GROUP BY cohort
ORDER BY avg_seconds ASC;

-- Top 10 advertisers by attributed conversions
SELECT
    advertiser_id,
    COUNT(*) AS attributed_conversions
FROM demo_data.affiliate_junction_<your-attendee-id>.kafka_events
WHERE event_type = 'conversion_identified'
GROUP BY advertiser_id
ORDER BY attributed_conversions DESC
LIMIT 10;


-- =============================================================================
-- Part 5 — More queries and a table check
-- =============================================================================

-- Fraud cookie volume
SELECT cookie_type, count(*) AS cnt,
       ROUND(100.0 * count(*) / sum(count(*)) OVER (), 1) AS pct
FROM demo_data.affiliate_junction_<your-attendee-id>.kafka_events
WHERE event_type = 'impression'
GROUP BY cookie_type;

-- Top publishers by impression volume
SELECT publisher_id, count(*) AS impressions
FROM demo_data.affiliate_junction_<your-attendee-id>.kafka_events
WHERE event_type = 'impression'
GROUP BY publisher_id ORDER BY impressions DESC LIMIT 10;

-- Does your table exist? (troubleshooting)
SHOW TABLES IN demo_data.affiliate_junction_<your-attendee-id>;
