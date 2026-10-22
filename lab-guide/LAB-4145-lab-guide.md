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
   - 6.1 [Set up watsonx.data Intelligence](#61-set-up-watsonxdata-intelligence)
   - 6.2 [Connect watsonx.data Intelligence to your table](#62-connect-watsonxdata-intelligence-to-your-table)
   - 6.3 [Run metadata enrichment](#63-run-metadata-enrichment)
   - 6.4 [Inspect generated metadata via MCP](#64-inspect-generated-metadata-via-mcp)
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
│   3 brokers · SASL_SSL · ~5 900 events (one batch)         │
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
│  demo_data.affiliate_junction_<your-id>.kafka_events          │
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
| Confluent Platform | 3 brokers (addresses on your credential sheet) · SASL_SSL | Shared — all attendees read the same topic |
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
    │                                │  SASL_SSL
    │                                └──────────► Confluent Platform
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
| `ATTENDEE_ID` | e.g. `student1` — assigned at lab start |
| IBM Bob licence key | Printed on your credential sheet |
| IBM Cloud login URL | Printed on your credential sheet |
| IBM Cloud username & password | Printed on your credential sheet |
| Kafka SASL password | Printed on your credential sheet |
| Kafka broker addresses | Printed on your credential sheet |
| watsonx.data instance CRN | Printed on your credential sheet |

The broker addresses and the instance CRN are the same for all attendees. The steps below show them as placeholders — replace each placeholder with the value from your credential sheet.

#### Log in to IBM Cloud and generate your API key

1. Open the URL from your credential sheet in a browser (e.g. `https://cloud.ibm.com`) and sign in with the student username and password provided.

2. Once logged in, navigate to **Manage → Access (IAM) → API Keys** using the top navigation bar.

3. Click **Create an IBM Cloud API key**.

4. Give it a name (e.g. `techxchange-lab-key`) and click **Create**.

5. **Copy the key immediately** — it is only shown once. Paste it somewhere safe (e.g. a text editor) before closing the dialog.

   > **Important:** If you close the dialog without copying the key, you will need to delete it and create a new one. The key cannot be retrieved after the dialog is closed.

You will use this API key in section 4.4 when configuring your `.env` file and in section 5.3 when setting up the MCP server connections.

### 4.2 Install IBM Bob and open the lab repository

IBM Bob is a desktop application. If you have not already installed it, download it now:

1. Go to **[bob.ibm.com/download](https://bob.ibm.com/download)** and download the installer for your operating system.
2. Run the installer and follow the prompts.
3. When prompted, enter the licence key from your credential sheet.

Once Bob is open, clone the lab repository using Bob's integrated terminal (`Ctrl+`` ` ``):

```bash
git clone https://github.com/arvy/TechXChange-LAB-4145.git
cd TechXChange-LAB-4145
```

Now open the cloned repository **as a folder** in Bob, so that the repository root becomes your workspace:

1. In Bob, use the **File** menu to open a folder (**Open Folder…**; on some systems the entry is called **Open…**).
2. Select the `TechXChange-LAB-4145` folder you just cloned (the folder that contains `pyproject.toml`, not a folder inside it or the folder above it), and confirm.
3. If Bob asks whether you trust this folder, choose to trust it.

> **Important:** Open the folder itself — do not open individual files from it. Bob loads the workspace MCP config (`.bob/mcp.json`) and resolves `${workspaceFolder}` from the open folder (see section 5.3), and the integrated terminal starts in it. Run all later commands in a terminal opened in this folder.

> **Already have Bob installed?** Skip the download — just open a new terminal, clone the repository, and open it as a folder.

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

Get the Confluent Platform CA certificate, `kafka-ca.crt`, and save it in the project root, the same folder as `pyproject.toml`. Your credential sheet tells you where to download it, or your instructor can give you the file. Without it the connection to Kafka fails.

Verify:

```bash
uv run python --version
# Python 3.11.x

ls -la kafka-ca.crt
# The file should be listed in the project root
```

### 4.4 Configure your environment files

The MCP ingestion server reads its credentials from `mcp_ingestion_server/.env`. A template is provided.

```bash
cp mcp_ingestion_server/env-sample mcp_ingestion_server/.env
```

Open `mcp_ingestion_server/.env` in Bob's editor and replace each `<…>` placeholder with the value from your credential sheet:

```
KAFKA_BOOTSTRAP_SERVERS=<broker addresses from your credential sheet, comma-separated>
KAFKA_TOPIC=affiliate-junction-events
KAFKA_SECURITY_PROTOCOL=SASL_SSL
KAFKA_SASL_MECHANISM=PLAIN
KAFKA_SASL_USERNAME=kafka-admin
KAFKA_SASL_PASSWORD=<your Kafka SASL password from TechZone>
KAFKA_SSL_CA_LOCATION=kafka-ca.crt

WATSONX_DATA_BASE_URL=https://console-ibm-cator.lakehouse.saas.ibm.com/lakehouse/api
WATSONX_DATA_API_KEY=<your IBM Cloud API key from TechZone>
WATSONX_DATA_INSTANCE_ID=<watsonx.data instance CRN from your credential sheet>
WATSONX_DATA_ENGINE_ID=presto44
WATSONX_DATA_TARGET_TABLE=demo_data.affiliate_junction_<your-attendee-id>.kafka_events
```

> **Important:** Replace `<your-attendee-id>` with your TechZone username (e.g. `student1`) in the `WATSONX_DATA_TARGET_TABLE` line. This namespaces your Iceberg table so attendees do not overwrite each other.

> **Note:** Your instructor has already published the events to the Kafka topic — you do not need to configure or start a producer.

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
CREATE SCHEMA IF NOT EXISTS demo_data.affiliate_junction_student1
WITH (location = 's3a://watsonx-data-76a38762-ab88-4c5a-91d7-9b32184e20c5/affiliate_junction_student1');
```

```sql
-- Step 2: Create the landing table
CREATE TABLE IF NOT EXISTS demo_data.affiliate_junction_student1.kafka_events (
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
SELECT * FROM demo_data.affiliate_junction_student1.kafka_events LIMIT 5;
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
      "cwd": "${workspaceFolder}",
      "alwaysAllow": ["subscribe_and_ingest"]
    }
  }
}
```

> **Note:** `${workspaceFolder}` is resolved automatically by Bob to the root of the open workspace — no manual path editing needed.

#### Step 2 — Get the patched Intelligence MCP server

The published `ibm-watsonx-data-intelligence-mcp-server` package has a bug that you will hit in Exercise 2. Use the patched source from the lab's fork instead. Clone it into your home directory, on the fix branch, and install its dependencies:

```bash
cd ~
git clone --branch fix/find-connection-id-personal-credentials https://github.com/arvy/data-intelligence-mcp-server.git
cd data-intelligence-mcp-server
uv sync
pwd
```

Copy the path printed by `pwd` (for example `/home/student1/data-intelligence-mcp-server`). In the next step, replace `<absolute-path-to>/data-intelligence-mcp-server` with it.

#### Step 3 — Global config (watsonx.data + Intelligence servers)

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
        "WATSONX_DATA_INSTANCE_ID": "<watsonx.data instance CRN from credential sheet>"
      },
      "disabled": false
    },
    "wxdi-mcp-server": {
      "command": "<absolute-path-to>/data-intelligence-mcp-server/.venv/bin/python",
      "args": ["-m", "app.main", "--transport", "stdio"],
      "cwd": "<absolute-path-to>/data-intelligence-mcp-server",
      "env": {
        "DI_SERVICE_URL": "https://api.ca-tor.dai.cloud.ibm.com",
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

> **Note:** Both servers use the same IBM Cloud API key from your credential sheet. The `watsonxdata` server uses `uvx` and downloads automatically on first run. The `wxdi-mcp-server` runs from the patched clone you made in Step 2. Replace `<absolute-path-to>/data-intelligence-mcp-server` in both the `command` and `cwd` lines with the path you copied. The Intelligence API endpoint for Toronto is `api.ca-tor.dai.cloud.ibm.com`, not the default `api.dataplatform.cloud.ibm.com`.

#### Step 4 — Reload Bob

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

Ask Bob in plain English:

```
Show me a sample of the live affiliate junction events coming off Kafka.
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

Now ask Bob to do the full pipeline. Start with a clear, natural request:

```
Ingest a batch of 50 affiliate junction events from Kafka into my Iceberg table.
```

Bob will figure out which tool to call and with what parameters. While it runs (usually 20–40 seconds), you will see:

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
  "target_table": "demo_data.affiliate_junction_student1.kafka_events"
}
```

Once that works, try an even more natural prompt that lets Bob choose sensible defaults entirely on its own:

```
Grab some live events from the Kafka topic and write them to watsonx.data.
```

> **Ingest more data:** Ask Bob to *"keep pulling events from Kafka and writing them to my table"* — Bob will loop the calls until you stop it or the topic is exhausted. A call that returns fewer rows than the batch size means the topic is empty. Load the whole topic before moving to Exercise 2. Exercise 3 queries conversions and fraud patterns, and those event types are only a small share of the stream, so a small load may contain no fraud events at all.
>
> **Keep each batch to 1 000 events or fewer** (for example: *"Ingest 1000 events at a time with a 45 second timeout, four times, and check the row count between calls."*). Very large batches can exceed the tool-call timeout. If a call times out, **do not simply retry** — the first call may still be running and a retry would load the same events twice. Check the row count first:
>
> ```sql
> SELECT count(*) AS total, count(DISTINCT event_id) AS distinct_events
> FROM demo_data.affiliate_junction_student1.kafka_events;
> ```
>
> If `total` is greater than `distinct_events`, you have duplicates. Run `DELETE FROM demo_data.affiliate_junction_student1.kafka_events;` and reload using a fresh `consumer_group_id`.

### 5.6 Verify the data in watsonx.data

Go back to the watsonx.data **Query Workspace** and run:

```sql
-- How many rows landed?
SELECT count(*) AS row_count
FROM demo_data.affiliate_junction_student1.kafka_events;
```

```sql
-- What event types do we have?
SELECT event_type, count(*) AS cnt
FROM demo_data.affiliate_junction_student1.kafka_events
GROUP BY event_type
ORDER BY cnt DESC;
```

```sql
-- Show 5 sample rows
SELECT event_id, event_type, publisher_id, cookie_type, cohort, ingested_at
FROM demo_data.affiliate_junction_student1.kafka_events
LIMIT 5;
```

You should see rows in all three event types, with `ingested_at` timestamps from the current session.

**Optional: double-check with Bob.** Ask Bob to run the same check through the watsonx.data MCP server and compare the answer with what the console showed:

```
How many rows are in my kafka_events table, and how many events of each type?
```

If the numbers differ from the console, check that both use the same schema (`affiliate_junction_<your student ID>`).

✅ **Exercise 1 complete.** You have built a live streaming ingestion pipeline controlled entirely by an AI agent via MCP.

---

## 6. Exercise 2 — Govern: Automated metadata cataloguing (~25 min)

In this exercise you will use **watsonx.data Intelligence** — connected to Bob via its own MCP server — to automatically catalogue the schema you just landed, assign business terms, and classify sensitive fields.

### 6.1 Set up watsonx.data Intelligence

Before Bob can run metadata imports, you need to complete three one-time setup steps in the UI and terminal.

#### Step 1 — Create your project in watsonx.data Intelligence

1. Open [cloud.ibm.com/authorize/e13cdedaed9949e9ac5fab1bfd2eeefb/1zRGU7wWWH](https://cloud.ibm.com/authorize/e13cdedaed9949e9ac5fab1bfd2eeefb/1zRGU7wWWH) and sign in with your TechZone credentials. This link logs you directly into the correct IBM Cloud account.
2. Once logged in, click **Resource list** in the left navigation.
3. Under the **AI / Machine Learning** category, find and click your **watsonx.data Intelligence** instance. This opens the watsonx.data Intelligence portal.
4. Click **Projects** in the left navigation, then **New project**.
5. Name it `Affiliate Junction - student1` (replace `student1` with your attendee ID) and click **Create**.

> **Note:** The project is where your metadata imports, enrichment jobs, and catalogued assets will live.

#### Step 2 — Create your watsonx.data Presto connection

The metadata import needs a connection to your watsonx.data instance. You will create it using the JSON export from the watsonx.data console.

**Get the connection JSON:**

1. Open the [watsonx.data console](https://console-ibm-cator.lakehouse.saas.ibm.com).
2. Go to **Infrastructure Manager** → click your **Starter** Presto engine → **Details**.
3. Click **Connect information** and copy the JSON from the **Connection details** field. It looks like:

```json
{
  "connection_name": "Starter",
  "properties": {
    "connection": [
      {"name": "host", "value": "console-ibm-cator.lakehouse.saas.ibm.com"},
      {"name": "port", "value": "443"},
      {"name": "cloud_resource_name", "value": "crn:v1:bluemix:..."},
      {"name": "deployment_type", "value": "saas"},
      ...
    ]
  }
}
```

**Create the connection in Intelligence:**

1. In your **Affiliate Junction - student1** project, click **Assets** → **New asset** → **Data access tools** → **Connection**.
2. Search for and select **IBM watsonx.data Presto**.
3. Click **Import connection values** and paste the JSON you copied above — the fields will auto-populate.
4. Under **Credentials**, enter your IBM Cloud API key (from section 4.1).
5. Name the connection `wx_presto_connect` and click **Create**.

#### Step 3 — Confirm the patched Intelligence MCP server

In section 5.3 you cloned the patched `data-intelligence-mcp-server` (the `fix/find-connection-id-personal-credentials` branch) and pointed the `wxdi-mcp-server` entry in `~/.bob/settings/mcp.json` at it. Nothing more to install here. If you skipped that step, go back to section 5.3 Steps 2 and 3 now, then reload Bob.

> **Why the patched version?** The published `ibm-watsonx-data-intelligence-mcp-server` package has a bug where metadata imports cannot find connections that use personal credentials. The bug is in `find_connection_id` in `app/services/tool_utils.py`: it used the `GET /v2/connections?project_id=...` list endpoint, which hides connections flagged as `personal_credentials`. The fix replaces it with a global search query (`POST /v3/search`) that correctly surfaces personal connections to their owner.

### 6.2 Connect watsonx.data Intelligence to your table

Bob now has access to the patched watsonx.data Intelligence MCP server. Ask Bob to find your asset:

```
Find my kafka_events table in watsonx.data Intelligence.
```

Bob will call the Intelligence MCP server's search tool and return the asset details.

If it is not yet catalogued, ask Bob to import it:

```
Import my kafka_events table from watsonx.data into the Intelligence catalogue.
```

Bob will:
1. Call `create_or_update_metadata_import` to register the import scoped to your table
2. Call `run_metadata_import` to execute it
3. Poll until the job completes and return the result

> **Watch for an empty import.** The import scope must include the catalog name: `/<catalog>/<schema>/<table>`, for example `/demo_data/affiliate_junction_student0/kafka_events`. Bob cannot reliably discover this path, because the path-listing tool returns nothing for this connection, so it may leave out the catalog. An import with the wrong scope still reports success, but it finds **0 assets**. Check that the result shows 1 imported asset. If it shows 0, tell Bob the full scope above and ask it to update the import and run it again.

### 6.3 Run metadata enrichment

Once the asset is imported, ask Bob to enrich it:

```
Analyse my kafka_events table — profile the data and suggest business terms for the columns.
```

Bob will:
1. Create a Metadata Enrichment area (MDE) if one doesn't exist
2. Add the `kafka_events` asset to it
3. Create an enrichment job with the `profile`, `assign_terms`, `dq_gen_constraints` (identify data quality checks) and `analyze_quality` (run data quality checks) objectives. Quality scores need **both** quality objectives: without generated checks, the run reports success but produces no scores.
4. Execute the job

Wait for the job to complete (typically 2–3 minutes). Ask Bob:

```
How is the enrichment job going?
```

The job assigns data classes from profiling, but it can only assign **business terms that already exist** in the glossary, and a new glossary is empty. The next step generates and publishes them.

#### Generate and publish business terms

1. Ask Bob to create a glossary category for your terms:

   ```
   Create a glossary category named "Affiliate Junction".
   ```

2. Ask Bob to generate draft terms from your columns:

   ```
   Generate business terms for kafka_events in the "Affiliate Junction" category.
   ```

   Bob runs term generation, and an LLM drafts one term per column, such as *Publisher Identifier* and *Cookie Type*. These are **drafts**. They are not usable until a person publishes them.

3. Add a business-level term of your own. The generated term "Kafka Events" comes from the table name, not from a column, so it describes the technology rather than the business concept. Ask Bob to create a better one:

   ```
   Create a business term "Affiliate Events" in the "Affiliate Junction" category with the description
   "Impression and conversion events recorded for affiliate marketing campaigns".
   ```

   Like the generated terms, this is a **draft**.

4. Publish the drafts through the governance workflow. Ask Bob:

   ```
   Show me the tasks in my workflow inbox.
   ```

   Then ask Bob to claim the task titled **Publish {artifactTypePlural}** and complete it. When Bob completes the task, a prompt asks **you** to choose approve or reject and to enter a comment. Bob does not make that decision for you.

   > **Insight:** Generated terms are suggestions. A human review step decides what enters the glossary. Bob can drive the workflow, but the approval is yours.

5. Ask Bob to re-run the enrichment job with the "Affiliate Junction" category included, so term assignment can match the published terms to your columns.

6. Assign your table-level term to the table:

   ```
   Assign the business term "Affiliate Events" to kafka_events.
   ```

   The table can now be found by the business keywords "affiliate events", even though its technical name is `kafka_events`.

### 6.4 Inspect generated metadata via MCP

Once enrichment completes, ask Bob to show you what was discovered:

```
Show me the data quality score for kafka_events in my project
```

```
What business terms and classifications are assigned to each column of kafka_events?
```

```
Show me the column details for kafka_events — I'm particularly interested
in the data classes assigned to cookie_id and publisher_id
```

Explore what the AI found:

- **Data classes**: Did it correctly identify `event_id` as a UUID? Did it classify `cookie_id` appropriately? See the review step below.
- **Business terms**: What terms were auto-assigned to columns like `advertiser_id`, `publisher_id`, `cohort`?
- **Data quality**: Which columns scored lowest? Compare the scores with how sparse each column really is.

> **Insight:** A quality score only reflects the checks that were generated. In a test run of 4 000 rows, `cohort` (about 8% NULL) had a completeness check and scored 92, while `conversion_id` (about 86% NULL) and `conversion_timestamp`, `impression_timestamp` and `time_to_conversion_seconds` (about 94% NULL) had no completeness check and scored 100. The NULLs are correct behaviour: each event type only populates its own fields. A score of 100 therefore does not mean a column is fully populated. To see the real fill rate, ask Bob to run a query for the share of NULLs per column. This is also a good discussion point for real-world data modelling: wide sparse tables vs. event-type-specific tables.

#### Review and correct the data classes

Data classes come from profiling a **sample** of your rows (1 000 rows by default), not the whole table. Because the sample changes between runs, the same column can get a different class each time. In our test runs `event_type` flipped between *Organization Name*, *Indicator* and *Text*, and `time_to_conversion_seconds` between *Boolean* and *Indicator*. *Organization Name* is wrong for a column of event names, and *Boolean* is wrong for a duration in seconds.

1. Ask Bob to list the data class assigned to each column of `kafka_events`, together with its confidence.
2. Pick the columns where the class looks wrong. Low confidence is a good hint.
3. Correct them in the watsonx.data Intelligence UI: open the `kafka_events` asset in your project, go to its column view and change the data class of each wrong column.
4. Ask Bob the same question again to confirm your corrections are in place. Bob's answer marks a class you set by hand as set by the user.

> **Insight:** AI-generated metadata is a starting point. Profiling samples the data, so it can be wrong or unstable, and a person has to review it before anyone relies on it.


✅ **Exercise 2 complete.** Your Iceberg table now has an automated metadata catalogue — business terms, data classes, and quality scores — without writing a single line of cataloguing code.

---

## 7. Exercise 3 — Query: Natural language analytics (~25 min)

In this exercise you will use Bob's natural language interface — backed by the watsonx.data MCP server — to run analytical queries against the affiliate junction events you ingested.

### 7.1 Ask questions about your data using Bob

Bob has direct access to the `execute_select` tool on the watsonx.data MCP server. You can ask questions in plain English.

Start with exploratory questions — no SQL required:

```
How many events of each type have I collected so far?
```

```
Which publishers are generating the most impressions?
```

```
Group those impressions by cohort instead.
```

> **Note:** Each publisher has only a handful of impressions, so the ranking from the previous question says little. The `cohort` column holds each publisher's industry (TECH, TRAVEL, HEALTH, FINANCE or FASHION), and grouping by it gives a view you can actually compare. When an answer is too fine-grained to be useful, ask Bob to group by a broader column.

```
What fraction of my events involve a fraud cookie?
```

Bob will translate each question into Presto SQL, execute it via the MCP server, and show you the results.

### 7.2 Run analytical queries via the watsonx.data MCP server

Try deeper questions:

```
On average, how long does it take for each publisher cohort to convert after seeing an ad?
```

```
Which cookie types are most likely to lead to a conversion?
```

```
Who are my top 10 advertisers by attributed conversions?
```

Bob will generate and execute SQL similar to:

```sql
SELECT
    cohort,
    ROUND(AVG(time_to_conversion_seconds), 1) AS avg_seconds,
    COUNT(*) AS conversions
FROM demo_data.affiliate_junction_student1.kafka_events
WHERE event_type = 'conversion_identified'
  AND cohort IS NOT NULL
GROUP BY cohort
ORDER BY avg_seconds ASC;
```

> **What to expect:** The cohorts should show clearly different averages. In this dataset TECH converts fastest and TRAVEL slowest, with FASHION, HEALTH and FINANCE in between.

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

> **What to expect:** The first question may come back empty. That is a valid result: each cookie keeps a single type, so none appears as both `fraud` and `cohort`. For the second question, look for a small group of publishers where nearly all impressions carry a fraud cookie. For the third question, each publisher-advertiser pair has only a few impressions, so adjust the threshold for "many impressions" to match your data.
>
> **Discussion:** The fraud patterns in the dataset are configurable in the producer. By default, there are 5 fraud cookies, each permanently tied to one publisher, and about 5% of impressions carry a fraud cookie. Ask your instructor how these parameters map to real-world affiliate fraud scenarios.

**Stretch goal — ingest more and re-query:**

If you finish early, ask Bob to load more data:

```
Pull in 500 more affiliate junction events from Kafka.
```

Then re-run your analytical queries to see how the results change with more data volume.

✅ **Exercise 3 complete.** You have queried a live Iceberg table using natural language, with Bob translating your questions into Presto SQL via MCP — no SQL knowledge required.

---

## 8. Did not finish the lab?

If you did not complete all exercises during the session, you can continue in the **self-paced lab room** (check the TechXchange schedule for times and location).

Your TechZone environment and watsonx.data schema will remain active for 48 hours after the session. The events stay in the Kafka topic, so you can load them again with a new `consumer_group_id`.

To resume, simply re-open Bob on your laptop, navigate to the `TechXChange-LAB-4145` folder, and continue — your `.env` files and virtualenv are still in place.

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

**Problem: `ingest_to_iceberg` times out, or `distinct_events` is lower than `total`**

Cause: The batch was too large for the tool-call timeout and the call was retried, so the same events were written twice.

Solution: Delete the rows (`DELETE FROM demo_data.affiliate_junction_student1.kafka_events;`), then ask Bob to ingest again in batches of 1 000 or fewer, using a new `consumer_group_id` (for example `student1-load-02`). Always check the row count before retrying a call that timed out.

---

**Problem: `ingest_to_iceberg` fails with `Commit failed: Local: No offset stored` and the table stays empty**

Cause: You used a new `consumer_group_id` together with a short `timeout_seconds`. The consumer did not receive any events before the timeout, so there was no offset to commit.

Solution: Ask Bob to repeat the call with a `timeout_seconds` of 45 or more. Check the row count first to confirm nothing was written.

---

**Problem: A tool call fails with `401` or `The Token has expired`**

Cause: The access token held by an MCP server has expired, usually after the session has been open for a while. The message can appear inside an otherwise normal response, for example as an error in an import result.

Solution: Reload the affected MCP server in Bob's MCP settings, then ask Bob to repeat the step. Check what the failed call changed before repeating it. A call that failed with a 401 did not complete.

---

**Problem: `subscribe_and_ingest` returns 0 events**

Cause: Your consumer group has already consumed all available messages.

Solution: Use a fresh `consumer_group_id` value — each unique group ID reads independently from the beginning of the topic:

```
Call subscribe_and_ingest with consumer_group_id="student1-fresh-01"
```

---

**Problem: `ingest_to_iceberg` returns `job_status: "failed"`**

Cause: Usually a SQL syntax error or a type mismatch.

Solution:
1. Check that your `WATSONX_DATA_TARGET_TABLE` in `.env` exactly matches the table you created (including your attendee ID).
2. Verify the table exists: go to Query Workspace and run `SHOW TABLES IN demo_data.affiliate_junction_student1`.
3. Try a manual SELECT first to confirm connectivity: ask Bob `Run SELECT count(*) FROM demo_data.affiliate_junction_student1.kafka_events using presto44`.

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
- **Lab repository:** github.com/arvy/TechXChange-LAB-4145

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
FROM demo_data.affiliate_junction_student1.kafka_events
GROUP BY event_type ORDER BY cnt DESC;

-- Fraud cookie volume
SELECT cookie_type, count(*) AS cnt,
       ROUND(100.0 * count(*) / sum(count(*)) OVER (), 1) AS pct
FROM demo_data.affiliate_junction_student1.kafka_events
WHERE event_type = 'impression'
GROUP BY cookie_type;

-- Average conversion time by cohort
SELECT cohort,
       count(*) AS conversions,
       ROUND(AVG(time_to_conversion_seconds), 0) AS avg_seconds
FROM demo_data.affiliate_junction_student1.kafka_events
WHERE event_type = 'conversion_identified'
GROUP BY cohort ORDER BY avg_seconds;

-- Top publishers by impression volume
SELECT publisher_id, count(*) AS impressions
FROM demo_data.affiliate_junction_student1.kafka_events
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
