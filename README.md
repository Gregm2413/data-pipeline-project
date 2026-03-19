# Olist Retail Intelligence Platform — Tech Stack Overview

This document explains the technologies used in the Retail Intelligence Platform project, why each was chosen, and what role it plays in the overall architecture. The project is intentionally built to mirror patterns found in real enterprise data engineering environments.

---

## Architecture Overview

The platform follows a **medallion architecture** — a layered data design pattern common in modern lakehouses. Raw source data flows through three progressively refined layers:

```
Source Database (PostgreSQL)
        ↓
   Ingestion Layer (Python)
        ↓
  Bronze Layer (raw Delta tables)
        ↓
  Silver Layer (cleaned, joined)
        ↓
  Gold Layer (aggregated, ML-ready)
```

Each layer adds value: bronze preserves source fidelity, silver enforces data quality and relationships, and gold serves analytics and machine learning use cases.

---

## Technology Breakdown

### PostgreSQL

**What it does:** Acts as the mock source transactional database — the system of record from which data is extracted before being loaded into the cloud data platform.

**Why we chose it:** In real enterprise environments, operational data lives in relational databases (order management systems, inventory systems, point-of-sale databases) rather than as raw files in cloud storage. PostgreSQL simulates this on-premises source system. It is open source, has excellent Python library support (SQLAlchemy, psycopg2), and has a well-maintained official Docker image that makes it trivial to spin up as a containerized service.

**Alternatives considered:** MySQL (equally valid), SQL Server (more realistic for a Microsoft-stack company but heavier to run locally), Oracle (most realistic for large enterprise but impractical for a local environment).

---

### Docker & Docker Compose

**What it does:** Containerizes the PostgreSQL database and the Python ingestion script into isolated, reproducible services. Docker Compose orchestrates both containers together and manages startup ordering.

**Why we chose it:** Running PostgreSQL inside Docker means the entire source environment is defined as code in `docker-compose.yml`. Anyone cloning the repository can reproduce the exact same environment with a single command (`docker compose up`) rather than manually installing and configuring a database server. This mirrors how services are deployed in modern enterprise infrastructure, where applications run in containers rather than directly on servers.

**Key design decision:** The `depends_on: service_healthy` directive in Docker Compose ensures PostgreSQL is fully ready before the ingestion script attempts to connect — a small but important production-grade detail.

---

### Python (ingest.py)

**What it does:** Reads the raw Olist CSV files, loads them into PostgreSQL, and adds lineage metadata columns to every table.

**Why we chose it:** Python is the de facto language of data engineering. The script uses `pandas` for data manipulation and `SQLAlchemy` with `psycopg2` for database connectivity — the standard stack for Python-to-PostgreSQL operations.

**Key design decisions:**
- `if_exists="replace"` makes every run idempotent — re-running the script produces the same result without duplicating data
- `_ingested_at` and `_source_file` columns are added to every table, establishing data lineage from the moment of ingestion
- Credentials are stored in a `.env` file and loaded via `python-dotenv`, never hardcoded

---

### Databricks Community Edition

**What it does:** Provides the cloud data platform where all transformation, storage, and analytics work happens. Notebooks run PySpark jobs against Delta tables stored in Unity Catalog.

**Why we chose it:** Databricks is one of the most widely adopted platforms in enterprise data engineering. It natively integrates with Delta Lake, supports both batch and streaming workloads, and is the standard platform for teams building on the lakehouse architecture pattern. Community Edition provides free access to a managed Spark environment without requiring cloud billing.

---

### Delta Lake

**What it does:** The open-source storage format underlying all tables in the platform. Delta Lake adds ACID transaction guarantees, schema enforcement, and time travel (version history) on top of Parquet files stored in cloud object storage.

**Why we chose it:** Delta Lake is the backbone of the Databricks lakehouse. Key capabilities relevant to this project:
- **Time travel** — ability to query or roll back to any previous version of a table
- **ACID transactions** — safe concurrent reads and writes
- **Schema enforcement** — rejects writes that don't match the defined schema, preventing silent data corruption
- **Upserts (MERGE)** — required for the silver layer to handle late-arriving or updated records

---

### Unity Catalog

**What it does:** Databricks' governance layer that organizes data into a three-level namespace: `catalog.schema.table`. All tables in this project live under the `olist` catalog.

**Why we chose it:** Unity Catalog is the current standard for managing data assets in Databricks workspaces. Organizing tables by layer (`olist.bronze`, `olist.silver`, `olist.gold`) reflects how data is structured in production environments and makes the lineage of data immediately visible from the table path alone.

---

### PySpark

**What it does:** The distributed data processing engine used inside Databricks notebooks to read CSVs, apply transformations, and write Delta tables.

**Why we chose it:** PySpark is the standard API for large-scale data transformation on Databricks. Even though the Olist dataset fits comfortably in memory, writing transformations in PySpark means the same code scales to datasets orders of magnitude larger without modification — a critical property for production data engineering work.

---

### Git & GitHub

**What it does:** Tracks all code changes with version history and hosts the repository publicly at `https://github.com/Gregm2413/data-pipeline-project`.

**Why we chose it:** Version control is non-negotiable in any professional engineering environment. Keeping notebooks exported and committed to Git ensures the project is reproducible, reviewable, and demonstrable to potential employers.

**Key design decision:** The `.gitignore` excludes `.env` (credentials) and `data/` (raw CSV files) from the repository — two artifacts that should never be committed to source control.

---

## What Is Not Yet in the Stack (Planned)

| Technology | Purpose |
|---|---|
| Kafka | Real-time behavioral event streaming |
| dbt Core | Modular, tested SQL transformations |
| Great Expectations | Data quality validation |
| GitHub Actions | CI/CD pipeline automation |
| MLflow | ML experiment tracking and model registry |
| Elasticsearch | Vector search and product embeddings |
| PyTorch | Neural network search ranking model |
| FastAPI | Model inference API layer |
| Kubernetes | Container orchestration for deployment |
| Streamlit | Analytics dashboard |

---

## Data Source

All data comes from the **Olist Brazilian E-Commerce dataset** (publicly available on Kaggle), consisting of 9 CSV files covering customers, orders, order items, payments, reviews, products, sellers, and geolocation. The dataset spans ~100,000 orders placed between 2016 and 2018.