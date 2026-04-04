# Olist Retail Intelligence Platform

A production-grade retail data engineering and ML portfolio project built on the [Olist Brazilian E-Commerce dataset](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce). Demonstrates end-to-end data pipeline development across ingestion, streaming, transformation, data quality, and machine learning — targeting real-time behavioral analytics use cases.

---

## Project Goal

Design and implement a scalable, production-ready data platform that ingests raw e-commerce data, simulates real-time behavioral event streams, transforms data through a medallion architecture, and surfaces business-ready analytics — all enforced with CI/CD, automated testing, and data quality checks.

---

## Target Use Case

This project directly mirrors the technical requirements of a **Data Engineer II — Real-Time Behavioral Analytics** role:

- Behavioral/event-based data pipelines with XDM-aligned schema
- Databricks + PySpark for large-scale transformation
- dbt Core for SQL-based gold layer modeling
- Kafka for real-time event streaming
- CI/CD with GitHub Actions and branch protection
- Medallion architecture (Bronze → Silver → Gold) on Delta Lake

---

## Repository

**GitHub:** https://github.com/Gregm2413/data-pipeline-project

---

## Tech Stack

| Layer | Tool |
|---|---|
| Local ingestion | Python, Docker, Docker Compose |
| Mock source database | PostgreSQL (Docker container) |
| Message broker | Kafka + Zookeeper (Docker containers) |
| Event generator | Python (`src/event_generator.py`) |
| Kafka consumer | Python (`src/kafka_consumer.py`) |
| Cloud data platform | Databricks Community Edition |
| Storage format | Delta Lake |
| Transformation engine | PySpark + dbt Core |
| Gold layer modeling | dbt-databricks adapter |
| CI/CD | GitHub Actions |
| Code quality | Ruff (linting), pytest (unit tests) |
| Version control | Git / GitHub |

---

## Architecture Overview

```
PostgreSQL (Docker)         Kafka (Docker)
  [Raw Olist CSVs]    →   [XDM Event Stream]
         |                        |
         ↓                        ↓
   Databricks Auto Loader   Kafka Consumer
         |                        |
         └─────────┬──────────────┘
                   ↓
          olist.bronze (Delta Lake)
                   |
                   ↓
          olist.silver (PySpark)
          - Sessionization
          - Funnel analytics
          - Customer journey
                   |
                   ↓
          olist.gold (dbt Core)
          - Business-ready models
          - Tested + documented
                   |
                   ↓
            CI/CD (GitHub Actions)
          - Lint, test, dbt compile
          - Branch protection on main
```

---

## Project Structure

```
data_pipeline_project/
├── .github/
│   └── workflows/
│       └── ci.yml                ← GitHub Actions CI pipeline
├── .env                          ← credentials (not in Git)
├── .gitignore
├── README.md
├── pyproject.toml                ← ruff + pytest configuration
├── docker-compose.yml            ← PostgreSQL + Zookeeper + Kafka
├── data/
│   └── raw/                      ← 9 Olist CSVs (not in Git)
├── databricks/
│   └── notebooks/
│       ├── 01_bronze.py          ← CSV → Delta bronze tables
│       ├── 02_bronze_events.py   ← Auto Loader → bronze events
│       └── 03_silver.py          ← bronze → silver transformations
├── olist_retail/                 ← dbt project
│   ├── dbt_project.yml
│   ├── profiles.yml
│   └── models/
│       └── gold/                 ← 4 gold dbt models
├── tests/
│   ├── test_event_generator.py   ← 20 unit tests
│   └── test_kafka_consumer.py    ← 18 unit tests
├── docker/
│   └── Dockerfile
└── src/
    ├── ingest.py
    ├── event_generator.py
    ← kafka_consumer.py
    ├── requirements.txt
    └── schemas/
        └── xdm_event_schema.json ← Adobe XDM-aligned event schema
```

---

## Phase-by-Phase Breakdown

### Phase 1 — Docker Foundation + Bronze Ingestion ✅

**What was built:**
Docker Compose environment with a PostgreSQL container acting as a mock on-premises OLTP source database. A Python ingestion script loads all 9 Olist CSVs into a PostgreSQL bronze schema with data lineage columns (`_ingested_at`, `_source_file`) automatically appended.

**Key design decisions:**
- PostgreSQL in Docker simulates a real-world on-prem source system — a common enterprise pattern where cloud pipelines must pull from legacy databases
- Lineage columns on every record establish auditability from day one
- `POSTGRES_HOST=postgres` in `.env` uses Docker's internal DNS for container-to-container networking, avoiding hardcoded IPs

**Talking points:**
- "I designed the ingestion layer to mirror an enterprise pattern where the source is a legacy on-prem database — not a file drop. This made the architecture more realistic and forced me to handle connection management, error handling, and lineage properly."
- "Adding `_ingested_at` and `_source_file` to every record at ingestion time means I always know the provenance of any row, which is critical when debugging data quality issues downstream."

---

### Phase 2 — Databricks + Delta Lake Medallion Architecture ✅

**What was built:**
Databricks Community Edition workspace with Unity Catalog structure (`olist` catalog, `olist.bronze` schema, `olist_raw` volume). Eight Olist CSVs loaded as Delta tables via PySpark notebook.

**Bronze table row counts:**

| Table | Rows |
|---|---|
| customers | 99,441 |
| orders | 99,441 |
| order_items | 112,650 |
| order_payments | 103,886 |
| order_reviews | 104,162 |
| products | 32,951 |
| sellers | 3,095 |
| product_category_translation | 71 |

**Key design decisions:**
- Delta Lake chosen for ACID transactions, time travel, and schema enforcement — production-grade requirements
- Unity Catalog provides fine-grained access control and data governance at the catalog/schema/table level
- Medallion architecture (Bronze → Silver → Gold) separates raw ingestion from transformation concerns, making each layer independently testable and replayable

**Talking points:**
- "I chose Delta Lake specifically because it gives you ACID guarantees on top of object storage, which is something you need the moment you have concurrent writers or need to replay a failed pipeline run without corrupting data."
- "Unity Catalog was a deliberate choice to demonstrate governance awareness — in a real company this is where you'd enforce who can see PII, which teams can write to which schemas, and so on."

---

### Phase 2.5 — Kafka Streaming Layer ✅

**What was built:**
Full real-time behavioral event streaming layer using Kafka and Zookeeper in Docker. An XDM-aligned event schema models behavioral events after Adobe's Experience Data Model. A Python event generator synthesizes realistic user sessions from the Olist order data and publishes them to a Kafka topic. A Kafka consumer batches events and writes them to JSON files, which are then ingested into Databricks via Auto Loader.

**Event funnel:**
`page_view → product_view → add_to_cart → [remove_from_cart] → purchase`

**Event counts in `olist.bronze.events`:**

| Event Type | Count |
|---|---|
| commerce.productViews | 6,899 |
| web.webpagedetails.pageViews | 2,816 |
| commerce.productListAdds | 146 |
| commerce.purchases | 135 |
| commerce.productListRemovals | 4 |

**Key design decisions:**
- XDM schema chosen specifically to demonstrate Adobe AEP familiarity — a preferred qualification for the target role
- ~19:1 browse-to-purchase session ratio produces a realistic ~5% conversion rate
- `trigger(availableNow=True)` in Auto Loader instead of continuous streaming — appropriate for Community Edition resource constraints while preserving the streaming architecture pattern
- `multiLine=true` in Auto Loader required because the consumer writes JSON arrays, not newline-delimited JSON

**Talking points:**
- "I modeled the event schema after Adobe XDM because that's the standard in enterprise customer journey analytics. If you're working with AEP or CJA, the field names and nesting conventions are the same — so the schema choice was intentional, not arbitrary."
- "The browse-to-purchase ratio was tuned to ~5% conversion, which reflects real e-commerce benchmarks. Synthetic data that doesn't look realistic undermines the value of any downstream analytics built on top of it."

---

### Phase 3 — Silver Layer Transformations (PySpark) ✅

**What was built:**
Silver layer in Databricks using PySpark. Three transformation notebooks reconstruct user sessions from raw events, compute funnel conversion rates step-by-step, and track cross-session customer journeys. The output mirrors what Adobe Customer Journey Analytics (CJA) produces natively.

**Transformations:**
- **Sessionization** — groups raw events into sessions using `session_id`, computing session duration, event counts, and conversion flags
- **Funnel analytics** — computes step-by-step conversion rates (view → add → purchase) across the full event population
- **Customer journey reconstruction** — tracks behavior across multiple sessions per customer, identifying re-engagement patterns and multi-touch paths to purchase

**Key design decisions:**
- Silver layer intentionally mirrors CJA output to demonstrate that the pipeline produces analytics-equivalent results without requiring a paid Adobe license
- PySpark window functions used for sessionization — the same approach that scales to billions of events in production

**Talking points:**
- "The silver layer is where raw events become analytically meaningful. I specifically structured the sessionization and funnel logic to produce outputs that match what you'd get from Adobe CJA — so a business analyst familiar with CJA reports would recognize the structure immediately."
- "Using window functions for sessionization is the production-correct approach. A naive GROUP BY would lose the ordering information you need to reconstruct event sequences correctly."

---

### Phase 4 — dbt Core Gold Layer ✅

**What was built:**
dbt Core project (`olist_retail`) with four gold models built on top of the silver Delta tables. All models tested with dbt's built-in schema tests. Gold tables written to `olist.gold` in Databricks via the `dbt-databricks` adapter.

**Key design decisions:**
- dbt chosen because it brings software engineering practices (version control, testing, documentation) to SQL transformations — the industry standard for analytics engineering
- Gold layer designed for direct consumption by BI tools or ML feature pipelines — no further transformation needed downstream
- Schema tests on every model catch breaking changes before they reach consumers

**Talking points:**
- "dbt forces you to treat SQL like software — every model is version controlled, every column can be documented, and every critical field has a test. That discipline is what separates ad-hoc SQL from a production transformation layer."
- "The gold layer is the contract with downstream consumers. By testing it in CI, I guarantee that if a model breaks, it fails loudly in the PR rather than silently in a dashboard."

---

### Phase 5 — CI/CD with GitHub Actions ✅

**What was built:**
GitHub Actions CI workflow that runs automatically on every pull request to main. Three parallel jobs enforce code quality and correctness gates before any merge. Main branch is protected — direct pushes are blocked, everything goes through a PR.

**CI jobs:**

| Job | What it does | Tool |
|---|---|---|
| Lint | Checks code style, import ordering, unused variables | Ruff |
| Unit Tests | Runs 38 tests covering event structure, funnel logic, batch writing, event validation | pytest |
| dbt Compile & Docs | Validates all SQL and model graph against live Databricks Unity Catalog; generates docs artifact | dbt-databricks |

**Key design decisions:**
- Ruff chosen over flake8 — written in Rust, dramatically faster, handles isort and pyupgrade in one tool
- dbt `compile` used instead of `dbt run` in CI — validates SQL syntax and ref resolution without executing against the warehouse, keeping CI fast
- `dbt docs generate` uploads the docs artifact to GitHub Actions on every PR, making model documentation always current
- GitHub Secrets used for Databricks credentials — never hardcoded, follows production security practices
- `profiles.yml` generated dynamically in CI from secrets — the file itself is gitignored to prevent credential leakage

**Talking points:**
- "I enforced branch protection on main from the start, even working solo. The habit of never pushing directly to main is what makes you a safe contributor on a team — you don't want to be the person who breaks the production pipeline because they skipped the PR."
- "The dbt compile step in CI is a lightweight but high-value gate. It catches broken SQL, unresolved refs, and schema mismatches without spinning up a full pipeline run. Fast feedback loops matter in CI."
- "38 unit tests run in under 30 seconds in CI because they're designed with no external dependencies — no Kafka, no Databricks, no PostgreSQL. Tests that depend on infrastructure are slow, flaky, and expensive. I isolated the logic and tested that instead."

---

## What's Next

| Phase | Focus |
|---|---|
| 6 | Data quality with Great Expectations |
| 7 | Traditional ML + MLflow (churn prediction, recommendations) |
| 8 | NLP, embeddings, vector search, Elasticsearch |
| 9 | Search ranking model with PyTorch |
| 10 | FastAPI inference layer |
| 11 | Kubernetes deployment |
| 12 | Streamlit dashboard + portfolio polish |

---

## Interview Talking Points — Summary

**On architecture:** "Every layer has a clear contract with the next. Bronze is raw and immutable. Silver is cleaned and enriched. Gold is business-ready and tested. If something breaks, you know exactly which layer to look at."

**On streaming:** "I built the streaming layer with XDM because that's the schema standard Adobe uses in AEP. The event field names, nesting, and commerce objects are all aligned — so a team already using AEP could consume these events directly."

**On CI/CD:** "I treat the pipeline code the same way I'd treat application code — linted, tested, and gated behind a PR. Data engineers who skip this are taking on invisible technical debt."

**On testing:** "I wrote 38 unit tests that run in 25 seconds with zero external dependencies. The goal wasn't coverage theater — it was testing the logic that actually matters: event structure, funnel ordering, batch file integrity."

**On data quality (Phase 6):** "The next layer I'm adding is Great Expectations to validate row counts, null rates, and value distributions at each pipeline stage. Silent bad data is worse than a pipeline failure — at least a failure you can see."