-- =============================================================================
-- Affiliate Junction — Iceberg Schema & Table DDL
-- Run these statements in the watsonx.data Query Workspace (Presto engine)
-- =============================================================================

-- Step 1: Create your personal schema.
-- Replace <your-attendee-id> with the ID assigned to you at lab start
-- (e.g. student0, student1, … student33)
-- This namespaces your tables so attendees don't overwrite each other.

CREATE SCHEMA IF NOT EXISTS demo_data.affiliate_junction_<your-attendee-id>
WITH (location = 's3a://watsonx-data-76a38762-ab88-4c5a-91d7-9b32184e20c5/affiliate_junction_<your-attendee-id>/');


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


-- Step 3: Verify the table was created
SELECT * FROM demo_data.affiliate_junction_<your-attendee-id>.kafka_events LIMIT 5;
