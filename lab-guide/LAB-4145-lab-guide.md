# LAB-4145: Hands-On: Agentic Data Pipelines with MCP and watsonx.data

---

**Hands-on lab guide**

| | |
|---|---|
| **Author** | Arvydas Jonusonis |
| **Email** | arvydas.jonusonis@ibm.com |
| **Role** | Solutions Architect, Data Labs |
| **Lab number** | LAB-4145 |
| **Duration** | 90 minutes |
| **TechXchange 2026** | |

---

## Table of Contents

1. [Introduction](#1-introduction)
2. [About this hands-on lab](#2-about-this-hands-on-lab)
3. [Lab architecture](#3-lab-architecture)
   - 3.1 [Pipeline overview](#31-pipeline-overview)
   - 3.2 [Infrastructure components](#32-infrastructure-components)
   - 3.3 [Lab environment connectivity](#33-lab-environment-connectivity)
4. [Getting started — environment setup](#4-getting-started--environment-setup)
   - 4.1 [Credentials and access](#41-credentials-and-access)
   - 4.2 [Clone the lab repository](#42-clone-the-lab-repository)
   - 4.3 [Install dependencies](#43-install-dependencies)
   - 4.4 [Configure your environment files](#44-configure-your-environment-files)
5. [Exercise 1 — Ingest: Stream Kafka events into watsonx.data (~25 min)](#5-exercise-1--ingest-stream-kafka-events-into-watsonxdata-25-min)
   - 5.1 [Create your personal Iceberg table](#51-create-your-personal-iceberg-table)
   - 5.2 [Explore the MCP ingestion server](#52-explore-the-mcp-ingestion-server)
   - 5.3 [Connect Bob to the MCP server](#53-connect-bob-to-the-mcp-server)
   - 5.4 [Consume your first batch from Kafka](#54-consume-your-first-batch-from-kafka)
   - 5.5 [Land events into your Iceberg table](#55-land-events-into-your-iceberg-table)
   - 5.6 [Verify the data in watsonx.data](#56-verify-the-data-in-watsonxdata)
6. [Exercise 2 — Govern: Automated metadata cataloguing (~25 min)](#6-exercise-2--govern-automated-metadata-cataloguing-25-min)
   - 6.1 [Connect watsonx.data Intelligence to your table](#61-connect-watsonxdata-intelligence-to-your-table)
   - 6.2 [Run metadata enrichment](#62-run-metadata-enrichment)
   - 6.3 [Inspect generated metadata via MCP](#63-inspect-generated-metadata-via-mcp)
7. [Exercise 3 — Query: Natural language analytics (~25 min)](#7-exercise-3--query-natural-language-analytics-25-min)
   - 7.1 [Ask questions about your data using Bob](#71-ask-questions-about-your-data-using-bob)
   - 7.2 [Run analytical queries via the watsonx.data MCP server](#72-run-analytical-queries-via-the-watsonxdata-mcp-server)
   - 7.3 [Fraud pattern analysis](#73-fraud-pattern-analysis)
8. [Did not finish the lab?](#8-did-not-finish-the-lab)
9. [Feedback](#9-feedback)
10. [Getting help and troubleshooting](#10-getting-help-and-troubleshooting)
    - 10.1 [Reach out to the lab instructor](#101-reach-out-to-the-lab-instructor)
    - 10.2 [Common troubleshooting tips](#102-common-troubleshooting-tips)
    - 10.3 [Getting help after the lab](#103-getting-help-after-the-lab)
11. [Lab reference materials](#11-lab-reference-materials)

---

## Notices and disclaimers

© 2026 International Business Machines Corporation. No part of this document may be reproduced or transmitted in any form without written permission from IBM.

U.S. Government Users Restricted Rights — use, duplication or disclosure restricted by GSA ADP Schedule Contract with IBM.

This document is current as of the initial date of publication and may be changed by IBM at any time. Not all offerings are available in every country in which IBM operates.

Information in these presentations has been reviewed for accuracy as of the date of initial publication and could include unintentional technical or typographical errors. IBM shall have no responsibility to update this information.

IBM, the IBM logo, and ibm.com are trademarks of International Business Machines Corporation, registered in many jurisdictions worldwide. Other product and service names might be trademarks of IBM or other companies. A current list of IBM trademarks is available at: www.ibm.com/legal/copytrade.shtml.

---

## 1. Introduction

The data industry is in the middle of a significant shift. For years, building a data pipeline required separate teams and tools for each stage: one team for ingestion, another for governance, another for analytics — each with its own interfaces, credentials, and mental model.

**Model Context Protocol (MCP)** changes this. MCP is an open standard that lets AI agents call external tools — databases, APIs, messaging systems — through a uniform interface. Instead of hand-crafting a bespoke integration for every connection, you expose capabilities as MCP tools and let an AI agent orchestrate them.

In this lab, you will see what that looks like in practice. You will use **IBM Bob** (IBM's AI-powered coding assistant, available as a desktop application) as your agent, and you will connect it to three MCP servers:

- An **ingestion server** you build yourself — reads from a live Confluent Kafka topic and writes directly to watsonx.data
- The **watsonx.data MCP server** — lets you run SQL, inspect schemas, and manage tables
- The **watsonx.data Intelligence MCP server** — exposes automated metadata cataloguing and governance

The use-case is **Affiliate Junction**: a realistic affiliate marketing event stream with publishers, advertisers, cookies, cohort profiles, and fraud signals — all flowing in real time.

By the end of this 90-minute lab, you will have built an end-to-end agentic data pipeline without writing a single integration layer by hand.

---

## 2. About this hands-on lab

This lab teaches you to:

- **Understand MCP** and why it is the right abstraction for agentic data pipelines
- **Build an MCP ingestion server** using the Python MCP SDK — a tool that bridges a Kafka topic to an Iceberg table in watsonx.data
- **Use watsonx.data as an open lakehouse** — create schemas, land Parquet/Iceberg data, and query it with Presto SQL
- **Trigger automated metadata governance** with watsonx.data Intelligence, and inspect what the AI catalogues about your data
- **Query your data in natural language** using IBM Bob connected to the watsonx.data MCP server

**Prerequisites:** No prior MCP or Kafka experience is required. Basic familiarity with Python and SQL is helpful.

**Timing:**

| Exercise | Duration | Outcome |
|---|---|---|
| Setup | ~10 min | Environment configured, repository cloned |
| Exercise 1 — Ingest | ~25 min | Live Kafka events landing in your Iceberg table |
| Exercise 2 — Govern | ~25 min | Automated metadata catalogue populated |
| Exercise 3 — Query | ~25 min | Natural language queries running against your data |
| Buffer | ~5 min | Q&A and stretch goals |

---

## 3. Lab architecture

### 3.1 Pipeline overview

The lab is built around a five-stage pipeline:

```
┌─────────────────────────────────────────────────────────────┐
│                    Confluent Platform                        │
│         Topic: affiliate-junction-events                    │
│   3 brokers · SASL_SSL · ~5 000 events/min                 │
└───────────────────────┬─────────────────────────────────────┘
                        │ Kafka consumer
                        ▼
┌─────────────────────────────────────────────────────────────┐
│         MCP Ingestion Server  (TechZone environment)        │
│  Tool: subscribe_and_ingest  — read batch from Kafka        │
│  Tool: ingest_to_iceberg     — write batch to Iceberg       │
└───────────────────────┬─────────────────────────────────────┘
                        │ INSERT via Presto HTTP API
                        ▼
┌─────────────────────────────────────────────────────────────┐
│            watsonx.data (Iceberg / Parquet)                  │
│  my_data.affiliate_junction_<your-id>.kafka_events          │
│  Presto engine: presto44                                    │
└────────────┬──────────────────────────┬──────────────────────┘
             │ metadata enrichment       │ SQL queries
             ▼                           ▼
┌────────────────────────┐   ┌──────────────────────────────┐
│  watsonx.data          │   │  IBM Bob (AI agent)           │
│  Intelligence          │   │  + watsonx.data MCP server   │
│  (MCP server)          │   │  Natural language → Presto   │
│  Auto-cataloguing &    │   │  SQL → results               │
│  data classes          │   │                              │
└────────────────────────┘   └──────────────────────────────┘
```

**Three event types flow through this pipeline:**

| Event type | When emitted | Key fields |
|---|---|---|
| `impression` | Publisher serves an ad | `impression_id`, `publisher_id`, `cookie_type`, `cohort` |
| `conversion` | A purchase occurs | `conversion_id`, `advertiser_id`, `cookie_id` |
| `conversion_identified` | Conversion attributed to an impression | All of the above + `time_to_conversion_seconds` |

### 3.2 Infrastructure components

This lab uses the **IBM TechZone "Confluent Platform Enterprise Software with watsonx.data portfolio"** environment. All infrastructure is pre-provisioned; each participant receives their own TechZone login.

| Resource | Details | Shared / Personal |
|---|---|---|
| Confluent Platform | 3 brokers at `163.66.90.46:9094–9096` · SASL_SSL | Shared — all attendees read the same topic |
| watsonx.data Premium | Toronto region · Presto engine `presto44` | Shared instance, personal schema per attendee |
| watsonx.data Intelligence | SaaS · automated enrichment | Shared instance |
| IBM Bob | AI coding assistant desktop app · license provided | Personal — installed on your laptop |
| Lab compute | Your laptop | Personal |

### 3.3 Lab environment connectivity

```
Your laptop
    │
    ▼
IBM Bob (desktop application)
    │  MCP stdio transport
    ├──────────────────────────► MCP Ingestion Server (local process)
    │                                │  SASL_SSL  port 9094/9095/9096
    │                                └──────────► Confluent Platform (163.66.90.46)
    │  MCP stdio transport
    ├──────────────────────────► watsonx.data MCP server
    │                                │  HTTPS  /lakehouse/api
    │                                └──────────► watsonx.data SaaS (ca-tor)
    │  MCP stdio transport
    └──────────────────────────► watsonx.data Intelligence MCP server
```

All credentials are provided via your TechZone environment. You will configure them into `.env` files in step 4.4.

---

## 4. Getting started — environment setup

> **Estimated time: ~10 minutes**

### 4.1 Credentials and access

Your lab instructor will provide a credential sheet with:

| Item | Where to find it |
|---|---|
| `ATTENDEE_ID` | e.g. `student01` — assigned at lab start |
| IBM Bob licence key | Printed on your credential sheet |
| IBM Cloud API key | Printed on your credential sheet |
| Kafka SASL password | Printed on your credential sheet |

All other values (broker address, instance CRN, etc.) are the same for all attendees and are pre-filled in the steps below.

### 4.2 Install IBM Bob and open the lab repository

IBM Bob is a desktop application. If you have not already installed it, download it now:

1. Go to **[bob.ibm.com/download](https://bob.ibm.com/download)** and download the installer for your operating system.
2. Run the installer and follow the prompts.
3. When prompted, enter the licence key from your credential sheet.

Once Bob is open, clone the lab repository using Bob's integrated terminal (`Ctrl+`` ` ``):

```bash
git clone https://github.ibm.com/arvydas-jonusonis/agentic-ai-data-pipeline-workshop.git
cd agentic-ai-data-pipeline-workshop
```

> **Already have Bob installed?** Skip the download — just open a new terminal and clone the repository.

### 4.3 Install dependencies

The project uses `uv` for Python environment management. Install it if it is not already on your machine:

```bash
# macOS / Linux
curl -LsSf https://astral.sh/uv/install.sh | sh

# Windows (PowerShell)
powershell -ExecutionPolicy ByPass -c "irm https://astral.sh/uv/install.ps1 | iex"
```

```bash
# Install all Python dependencies into a local virtualenv
uv sync
```

Copy the Confluent Platform CA certificate into the project root (pre-downloaded in your environment):

```bash
cp ~/kafka-ca.crt ./kafka-ca.crt
```

Verify:

```bash
uv run python --version
# Python 3.11.x
```

### 4.4 Configure your environment files

The MCP ingestion server reads its credentials from `mcp_ingestion_server/.env`. A template is provided.

```bash
cp mcp_ingestion_server/env-sample mcp_ingestion_server/.env
```

Open `mcp_ingestion_server/.env` in Bob's editor and fill in the two values from your credential sheet:

```
KAFKA_BOOTSTRAP_SERVERS=163.66.90.46:9094,163.66.90.46:9095,163.66.90.46:9096
KAFKA_TOPIC=affiliate-junction-events
KAFKA_SECURITY_PROTOCOL=SASL_SSL
KAFKA_SASL_MECHANISM=PLAIN
KAFKA_SASL_USERNAME=kafka-admin
KAFKA_SASL_PASSWORD=<your Kafka SASL password from TechZone>
KAFKA_SSL_CA_LOCATION=kafka-ca.crt

WATSONX_DATA_BASE_URL=https://console-ibm-cator.lakehouse.saas.ibm.com/lakehouse/api
WATSONX_DATA_API_KEY=<your IBM Cloud API key from TechZone>
WATSONX_DATA_INSTANCE_ID=crn:v1:bluemix:public:lakehouse:ca-tor:a/13821c80b84a4240820821b6b40185c9:8afc678c-3e67-4589-ad0d-ee93c8346dc4::
WATSONX_DATA_ENGINE_ID=presto44
WATSONX_DATA_TARGET_TABLE=my_data.affiliate_junction_<your-attendee-id>.kafka_events
```

> **Important:** Replace `<your-attendee-id>` with your TechZone username (e.g. `student01`) in the `WATSONX_DATA_TARGET_TABLE` line. This namespaces your Iceberg table so attendees do not overwrite each other.

> **Note:** The producer is already running centrally — you do not need to configure or start it.

---

## 5. Exercise 1 — Ingest: Stream Kafka events into watsonx.data (~25 min)

In this exercise you will:
1. Create your personal Iceberg table in watsonx.data
2. Explore the MCP ingestion server code
3. Connect IBM Bob to the MCP server
4. Use Bob to consume a batch of events from Kafka
5. Use Bob to insert those events into your Iceberg table
6. Verify the rows landed using SQL

### 5.1 Create your personal Iceberg table

Open the [watsonx.data console](https://console-ibm-cator.lakehouse.saas.ibm.com) and navigate to **Query Workspace**.

Run the following SQL, replacing `<your-attendee-id>` with your assigned ID:

```sql
-- Step 1: Create your personal schema
CREATE SCHEMA IF NOT EXISTS my_data.affiliate_junction_student01
WITH (location = 's3a://my-data/affiliate_junction_student01/');
```

```sql
-- Step 2: Create the landing table
CREATE TABLE IF NOT EXISTS my_data.affiliate_junction_student01.kafka_events (
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

```sql
-- Step 3: Verify
SELECT * FROM my_data.affiliate_junction_student01.kafka_events LIMIT 5;
-- Should return: (no rows) — table is empty for now
```

> **Tip:** The DDL file is also available at `schema/kafka_events.sql` in the repository. The partitioning by `day(event_timestamp)` and `event_type` ensures efficient queries when filtering by time or event type.

### 5.2 Explore the MCP ingestion server

Open `mcp_ingestion_server/server.py` in your editor. Take a few minutes to read through it. You will see:

- **`subscribe_and_ingest`** — a `@mcp.tool()` function that creates a Kafka consumer, polls up to `batch_size` messages, parses each one with `_parse_message`, and returns a structured list of events. It commits offsets only after a successful batch.

- **`ingest_to_iceberg`** — calls `subscribe_and_ingest` internally, then builds an `INSERT INTO … VALUES` SQL statement and submits it to watsonx.data via the Presto HTTP API (`/v3/v1/statement`). It polls for completion and returns the job status.

- **`_parse_message`** — flattens the Kafka JSON envelope into 15 columns matching the Iceberg schema. Fields absent in a given event type become `NULL`.

**Key design decisions to understand:**

| Decision | Why |
|---|---|
| One table for all 3 event types | Simplifies ingestion; NULL fields for non-applicable columns |
| Offset commit after batch | Ensures at-least-once delivery; duplicates are rare and detectable via `event_id` |
| Presto HTTP API (not JDBC) | No driver install needed; works from any Python environment |
| Per-attendee consumer group | Each attendee's Bob instance reads independently from offset 0 |

### 5.3 Connect Bob to the MCP servers

This lab uses three MCP servers. You configure them in two places:

| MCP server | Config location | Why |
|---|---|---|
| `affiliate-junction-ingestion` | Workspace (`.bob/mcp.json`) | Points to your local virtualenv — path is project-specific |
| `watsonxdata` | Global (`~/.bob/settings/mcp.json`) | Shared watsonx.data credentials, available in all projects |
| `wxdi-mcp-server` | Global (`~/.bob/settings/mcp.json`) | Shared watsonx.data Intelligence credentials |

#### Step 1 — Workspace config (ingestion server)

The workspace config is already in the repository. Check it:

```bash
cat .bob/mcp.json
```

It should contain:

```json
{
  "mcpServers": {
    "affiliate-junction-ingestion": {
      "command": ".venv/bin/python",
      "args": ["-m", "mcp_ingestion_server.server"],
      "cwd": "/home/<your-attendee-id>/agentic-ai-data-pipeline-workshop",
      "alwaysAllow": ["subscribe_and_ingest"]
    }
  }
}
```

> **Note:** Update `cwd` to the absolute path of the repository on your machine. Run `pwd` in the terminal to get it.

#### Step 2 — Global config (watsonx.data + Intelligence servers)

Open (or create) `~/.bob/settings/mcp.json` and add the two server entries below. If the file already exists, merge the `mcpServers` block — do not overwrite the whole file.

```json
{
  "mcpServers": {
    "watsonxdata": {
      "command": "uvx",
      "args": ["ibm-watsonxdata-mcp-server", "--transport", "stdio"],
      "env": {
        "WATSONX_DATA_BASE_URL": "https://console-ibm-cator.lakehouse.saas.ibm.com/lakehouse/api",
        "WATSONX_DATA_API_KEY": "<your IBM Cloud API key from credential sheet>",
        "WATSONX_DATA_INSTANCE_ID": "crn:v1:bluemix:public:lakehouse:ca-tor:a/13821c80b84a4240820821b6b40185c9:8afc678c-3e67-4589-ad0d-ee93c8346dc4::"
      },
      "disabled": false
    },
    "wxdi-mcp-server": {
      "command": "uvx",
      "args": ["ibm-watsonx-data-intelligence-mcp-server", "--transport", "stdio"],
      "env": {
        "DI_SERVICE_URL": "https://api.dataplatform.cloud.ibm.com",
        "DI_APIKEY": "<your IBM Cloud API key from credential sheet>",
        "DI_ENV_MODE": "SaaS",
        "LOG_FILE_PATH": "/tmp/di-mcp-server-logs"
      },
      "disabled": false,
      "alwaysAllow": []
    }
  }
}
```

> **Note:** Both servers use the same IBM Cloud API key from your credential sheet. `uvx` runs each server in an isolated environment — no manual install needed. The first run will download the package automatically (requires internet access).

#### Step 3 — Reload Bob

Reload Bob to pick up all three servers: open the command palette (`Ctrl+Shift+P`) and run **Developer: Reload Window**.

Verify Bob can see all the tools by asking it:

```
What MCP servers and tools do you have available?
```

Bob should list tools from all three servers:
- `affiliate-junction-ingestion` → `subscribe_and_ingest`, `ingest_to_iceberg`
- `watsonxdata` → `execute_select`, `execute_insert`, `list_schemas`, `list_tables`, …
- `wxdi-mcp-server` → `search_asset`, `create_metadata_import`, `execute_metadata_enrichment_asset`, …

### 5.4 Consume your first batch from Kafka

Ask Bob:

```
Call subscribe_and_ingest with batch_size=10, timeout_seconds=15,
and consumer_group_id="student01-explore"
```

Bob will call the tool and return a result like:

```json
{
  "events": [
    {
      "event_id": "a1b2c3d4-...",
      "event_type": "impression",
      "publisher_id": "PID_TECH_001",
      "cookie_type": "cohort",
      "cohort": "TECH",
      ...
    },
    ...
  ],
  "count": 10,
  "topic": "affiliate-junction-events",
  "errors": []
}
```

Take a moment to explore the events Bob returned. Notice:

- All three event types may appear in a single batch (`impression`, `conversion`, `conversion_identified`)
- `conversion_identified` events have `time_to_conversion_seconds` populated; the others have `NULL` there
- The `cookie_type` field explicitly labels each cookie as `cohort`, `fraud`, or `random` — no string parsing needed

> **What just happened?** Bob called your MCP server, which opened a Kafka consumer, subscribed to `affiliate-junction-events`, polled 10 messages, and committed the offset. The events are now structured Python dicts — but they haven't been written to Iceberg yet.

### 5.5 Land events into your Iceberg table

Now ask Bob to do the full pipeline in one call:

```
Call ingest_to_iceberg with batch_size=50, timeout_seconds=20,
and consumer_group_id="student01-ingest"
```

Bob will call the tool. While it runs (usually 20–40 seconds), you will see:

1. A Kafka consumer opens and polls 50 events
2. An `INSERT INTO … VALUES` statement is generated with 50 rows
3. An IAM token is exchanged
4. The INSERT is submitted to watsonx.data's Presto engine
5. The tool polls until the job reaches `FINISHED`

The response should be:

```json
{
  "rows_written": 50,
  "kafka_count": 50,
  "kafka_errors": [],
  "job_status": "completed",
  "target_table": "my_data.affiliate_junction_student01.kafka_events"
}
```

> **Ingest a larger batch:** Once you've confirmed this works, try `batch_size=200` to load more data for the analysis exercises.

### 5.6 Verify the data in watsonx.data

Go back to the watsonx.data **Query Workspace** and run:

```sql
-- How many rows landed?
SELECT count(*) AS row_count
FROM my_data.affiliate_junction_student01.kafka_events;
```

```sql
-- What event types do we have?
SELECT event_type, count(*) AS cnt
FROM my_data.affiliate_junction_student01.kafka_events
GROUP BY event_type
ORDER BY cnt DESC;
```

```sql
-- Show 5 sample rows
SELECT event_id, event_type, publisher_id, cookie_type, cohort, ingested_at
FROM my_data.affiliate_junction_student01.kafka_events
LIMIT 5;
```

You should see rows in all three event types, with `ingested_at` timestamps from the current session.

✅ **Exercise 1 complete.** You have built a live streaming ingestion pipeline controlled entirely by an AI agent via MCP.

---

## 6. Exercise 2 — Govern: Automated metadata cataloguing (~25 min)

In this exercise you will use **watsonx.data Intelligence** — connected to Bob via its own MCP server — to automatically catalogue the schema you just landed, assign business terms, and classify sensitive fields.

### 6.1 Connect watsonx.data Intelligence to your table

Bob already has access to the watsonx.data Intelligence MCP server (`wxdi-mcp-server`). Ask Bob to find your asset:

```
Search for the asset "kafka_events" in the affiliate_junction_student01 project or catalog
```

Bob will call the Intelligence MCP server's search tool and return the asset details.

If it is not yet catalogued, ask Bob to import it:

```
Create a metadata import for the kafka_events table in watsonx.data.
Use the presto44 connection and scope my_data.affiliate_junction_student01
```

Bob will:
1. Call `create_metadata_import` to register the import
2. Call `execute_metadata_import` to run it
3. Return the job monitoring URL

### 6.2 Run metadata enrichment

Once the asset is imported, ask Bob to enrich it:

```
Run metadata enrichment on the kafka_events table.
Profile the data and assign business terms.
Use the [your instructor's category] category.
```

Bob will:
1. Create a Metadata Enrichment area (MDE) if one doesn't exist
2. Add the `kafka_events` asset to it
3. Create an enrichment job with `profile` and `assign_terms` objectives
4. Execute the job

Wait for the job to complete (typically 2–3 minutes). Ask Bob:

```
What is the status of the metadata enrichment job?
```

### 6.3 Inspect generated metadata via MCP

Once enrichment completes, ask Bob to show you what was discovered:

```
Show me the data quality score for kafka_events in my project
```

```
What business terms and classifications were assigned to kafka_events?
```

```
Show me the column details for kafka_events — I'm particularly interested
in the data classes assigned to cookie_id and publisher_id
```

Explore what the AI found:

- **Data classes**: Did it correctly identify `event_id` as a UUID? Did it classify `cookie_id` appropriately?
- **Business terms**: What terms were auto-assigned to columns like `advertiser_id`, `publisher_id`, `cohort`?
- **Data quality**: Are there completeness issues (expected — many nullable fields are NULL for non-applicable event types)?

> **Insight:** The fields like `impression_id` and `conversion_id` will show low completeness scores — this is correct behaviour. Each event type only populates its own fields. This is a good discussion point for real-world data modelling: wide sparse tables vs. event-type-specific tables.

✅ **Exercise 2 complete.** Your Iceberg table now has an automated metadata catalogue — business terms, data classes, and quality scores — without writing a single line of cataloguing code.

---

## 7. Exercise 3 — Query: Natural language analytics (~25 min)

In this exercise you will use Bob's natural language interface — backed by the watsonx.data MCP server — to run analytical queries against the affiliate junction events you ingested.

### 7.1 Ask questions about your data using Bob

Bob has direct access to the `execute_select` tool on the watsonx.data MCP server. You can ask questions in plain English.

Start with exploratory questions:

```
How many events of each type are in my kafka_events table?
Use my_data.affiliate_junction_student01 schema.
```

```
What are the top 5 publishers by impression volume?
```

```
What percentage of events have a fraud cookie?
```

Bob will translate each question into Presto SQL, execute it via the MCP server, and show you the results.

### 7.2 Run analytical queries via the watsonx.data MCP server

Try more analytical questions:

```
What is the average time to conversion in seconds for each publisher cohort?
Only look at conversion_identified events.
```

```
Show me the conversion rate by cookie type — that is, for each cookie_type,
what fraction of cookies that appear in impressions also appear in
conversion_identified events?
```

```
Which advertisers have the highest number of attributed conversions?
Show the top 10.
```

Bob will generate and execute SQL similar to:

```sql
SELECT
    cohort,
    ROUND(AVG(time_to_conversion_seconds), 1) AS avg_seconds,
    COUNT(*) AS conversions
FROM my_data.affiliate_junction_student01.kafka_events
WHERE event_type = 'conversion_identified'
  AND cohort IS NOT NULL
GROUP BY cohort
ORDER BY avg_seconds ASC;
```

Review the SQL Bob generates. You can ask it to explain or modify the query:

```
Can you add a filter to only include conversions that happened
within 30 minutes (1800 seconds)?
```

### 7.3 Fraud pattern analysis

The Affiliate Junction dataset includes simulated fraud signals. Ask Bob to help you find them:

```
Are there any cookie_ids that appear in both fraud and cohort cookie_types?
This would indicate cross-contamination.
```

```
Show me publishers that have an unusually high ratio of fraud cookies
compared to their total impression volume.
```

```
Which publisher-advertiser pairs have zero conversion_identified events
despite having many impressions? These might indicate attribution problems.
```

> **Discussion:** The fraud patterns in the dataset are configurable in the producer. By default, 5% of impressions carry a fraud cookie, and 5 publishers are permanently assigned fraud cookies. Ask your instructor how these parameters map to real-world affiliate fraud scenarios.

**Stretch goal — ingest more and re-query:**

If you finish early, ask Bob to ingest several more batches:

```
Call ingest_to_iceberg 5 times with batch_size=100,
using a unique consumer_group_id each time (e.g. student01-batch-1 through student01-batch-5)
```

Then re-run your analytical queries to see how the results change with more data volume.

✅ **Exercise 3 complete.** You have queried a live Iceberg table using natural language, with Bob translating your questions into Presto SQL via MCP — no SQL knowledge required.

---

## 8. Did not finish the lab?

If you did not complete all exercises during the session, you can continue in the **self-paced lab room** (check the TechXchange schedule for times and location).

Your TechZone environment and watsonx.data schema will remain active for 48 hours after the session. The Kafka topic will continue receiving events from the shared producer.

To resume, simply re-open Bob on your laptop, navigate to the `agentic-ai-data-pipeline-workshop` folder, and continue — your `.env` files and virtualenv are still in place.

---

## 9. Feedback

Your feedback helps IBM improve future TechXchange labs. Please take 2 minutes to complete the survey at the end of the session — your instructor will provide the QR code or URL.

Things we especially want to know:
- Was the pacing right for 90 minutes?
- Were the exercises clear and well-sequenced?
- What would you add or change?

---

## 10. Getting help and troubleshooting

### 10.1 Reach out to the lab instructor

Raise your hand at any time. Instructors are circulating throughout the lab room and will assist promptly.

### 10.2 Common troubleshooting tips

**Problem: Bob does not list the `affiliate-junction-ingestion` tools**

Cause: The MCP server was not loaded at Bob startup.

Solution:
1. Verify `.bob/mcp.json` exists and the `cwd` path matches your home directory in the TechZone environment.
2. Open the Bob command palette (`Ctrl+Shift+P`) and run **Developer: Reload Window**.
3. Ask Bob again: `What MCP tools do you have?`

---

**Problem: `subscribe_and_ingest` returns 0 events**

Cause: Your consumer group has already consumed all available messages.

Solution: Use a fresh `consumer_group_id` value — each unique group ID reads independently from the beginning of the topic:

```
Call subscribe_and_ingest with consumer_group_id="student01-fresh-01"
```

---

**Problem: `ingest_to_iceberg` returns `job_status: "failed"`**

Cause: Usually a SQL syntax error or a type mismatch.

Solution:
1. Check that your `WATSONX_DATA_TARGET_TABLE` in `.env` exactly matches the table you created (including your attendee ID).
2. Verify the table exists: go to Query Workspace and run `SHOW TABLES IN my_data.affiliate_junction_student01`.
3. Try a manual SELECT first to confirm connectivity: ask Bob `Run SELECT count(*) FROM my_data.affiliate_junction_student01.kafka_events using presto44`.

---

**Problem: Kafka connection error / SSL handshake failure**

Cause: Missing or incorrect CA certificate.

Solution:
```bash
# Verify the cert is in place
ls -la kafka-ca.crt

# Check it is a valid cert
openssl x509 -in kafka-ca.crt -noout -subject

# Verify KAFKA_SSL_CA_LOCATION in your .env points to it
grep SSL mcp_ingestion_server/.env
```

---

**Problem: `401 Unauthorized` when writing to watsonx.data**

Cause: API key expired or incorrectly copied.

Solution: Ask your instructor for a fresh API key. Update `WATSONX_DATA_API_KEY` in `mcp_ingestion_server/.env` and reload Bob's MCP server.

---

**Problem: Schema already exists error when running DDL**

This is safe to ignore — the `CREATE SCHEMA IF NOT EXISTS` and `CREATE TABLE IF NOT EXISTS` statements are idempotent.

---

### 10.3 Getting help after the lab

- **IBM TechXchange community:** community.ibm.com/techxchange
- **watsonx.data documentation:** ibm.biz/wxdata-docs
- **MCP SDK documentation:** modelcontextprotocol.io
- **Lab repository:** github.ibm.com/arvydas-jonusonis/agentic-ai-data-pipeline-workshop

---

## 11. Lab reference materials

### Key concepts

| Term | Definition |
|---|---|
| **MCP (Model Context Protocol)** | Open standard for exposing tools and data sources to AI agents via a uniform interface |
| **FastMCP** | Python decorator-based framework for building MCP servers (v1 API) |
| **Iceberg** | Open table format for large-scale analytics; supports schema evolution, time travel, and ACID transactions |
| **Parquet** | Columnar storage format used by Iceberg for efficient analytical queries |
| **Presto** | Distributed SQL query engine; watsonx.data uses Presto for interactive queries |
| **Kafka consumer group** | A named group of consumers that collectively read a Kafka topic; each group tracks its own offsets independently |
| **Attribution window** | The time window (90 minutes here) within which an impression can be matched to a conversion |
| **IAM token** | Short-lived bearer token issued by IBM Cloud IAM in exchange for an API key |

### Data schema reference

```
kafka_events table — 15 columns
─────────────────────────────────────────────────────────────
event_id                    varchar    — UUID, unique per event
event_type                  varchar    — impression | conversion | conversion_identified
event_timestamp             timestamp  — when the event occurred (UTC)
schema_version              varchar    — "1.0"
impression_id               varchar    — impression events only
conversion_id               varchar    — conversion / conversion_identified only
publisher_id                varchar    — format: PID_{COHORT}_{NNN}
advertiser_id               varchar    — format: AID_{NNNNNN}
cookie_id                   varchar    — format: CID_{TYPE}_{NNNNNN}
cookie_type                 varchar    — cohort | fraud | random
cohort                      varchar    — TECH | FASHION | HEALTH | FINANCE | TRAVEL
impression_timestamp        timestamp  — conversion_identified only
conversion_timestamp        timestamp  — conversion_identified only
time_to_conversion_seconds  bigint     — conversion_identified only
ingested_at                 timestamp  — when the row was written to Iceberg
```

### Useful SQL snippets

```sql
-- Event type distribution
SELECT event_type, count(*) AS cnt
FROM my_data.affiliate_junction_student01.kafka_events
GROUP BY event_type ORDER BY cnt DESC;

-- Fraud cookie volume
SELECT cookie_type, count(*) AS cnt,
       ROUND(100.0 * count(*) / sum(count(*)) OVER (), 1) AS pct
FROM my_data.affiliate_junction_student01.kafka_events
WHERE event_type = 'impression'
GROUP BY cookie_type;

-- Average conversion time by cohort
SELECT cohort,
       count(*) AS conversions,
       ROUND(AVG(time_to_conversion_seconds), 0) AS avg_seconds
FROM my_data.affiliate_junction_student01.kafka_events
WHERE event_type = 'conversion_identified'
GROUP BY cohort ORDER BY avg_seconds;

-- Top publishers by impression volume
SELECT publisher_id, count(*) AS impressions
FROM my_data.affiliate_junction_student01.kafka_events
WHERE event_type = 'impression'
GROUP BY publisher_id ORDER BY impressions DESC LIMIT 10;
```

### Further learning

| Resource | URL |
|---|---|
| watsonx.data product page | ibm.com/products/watsonx-data |
| watsonx.data documentation | ibm.biz/wxdata-docs |
| MCP specification | modelcontextprotocol.io/specification |
| MCP Python SDK | github.com/modelcontextprotocol/python-sdk |
| Apache Iceberg documentation | iceberg.apache.org |
| Confluent Platform documentation | docs.confluent.io |
| TechXchange 2026 lab downloads | ibm.biz/techxchange2026-labs |

---

*End of lab guide — LAB-4145*

*© 2026 International Business Machines Corporation*
