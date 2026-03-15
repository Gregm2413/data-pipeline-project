"""
ingest.py
---------
Phase 1 ingestion script.

Reads Olist CSV files from the local data/raw directory and loads them
into PostgreSQL as raw bronze-layer tables. Each table is loaded with
an ingestion timestamp column appended so we have lineage from day one.

Usage:
    docker compose up ingestion
    (or locally) python src/ingest.py

Olist dataset download:
    https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce
    Place all CSVs in data/raw/ before running.
"""

import os
from pathlib import Path
from datetime import datetime, timezone

import pandas as pd
from sqlalchemy import create_engine, text
from loguru import logger


# ─────────────────────────────────────────────
# Config - read from environment variables
# (set in .env and passed through docker-compose)
# ─────────────────────────────────────────────
POSTGRES_USER = os.environ["POSTGRES_USER"]
POSTGRES_PASSWORD = os.environ["POSTGRES_PASSWORD"]
POSTGRES_DB = os.environ["POSTGRES_DB"]
POSTGRES_HOST = os.environ.get("POSTGRES_HOST", "localhost")
POSTGRES_PORT = os.environ.get("POSTGRES_PORT", "5432")
DATA_DIR = Path(os.environ.get("DATA_DIR", "data/raw"))

DATABASE_URL = (
    f"postgresql+psycopg2://{POSTGRES_USER}:{POSTGRES_PASSWORD}"
    f"@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
)

# ─────────────────────────────────────────────
# Olist CSV files and their target table names
# All land in the "bronze" schema in Postgres
# ─────────────────────────────────────────────
OLIST_FILES = {
    "olist_customers_dataset.csv": "customers",
    "olist_orders_dataset.csv": "orders",
    "olist_order_items_dataset.csv": "order_items",
    "olist_order_payments_dataset.csv": "order_payments",
    "olist_order_reviews_dataset.csv": "order_reviews",
    "olist_products_dataset.csv": "products",
    "olist_sellers_dataset.csv": "sellers",
    "olist_geolocation_dataset.csv": "geolocation",
    "product_category_name_translation.csv": "product_category_translation",
}

BRONZE_SCHEMA = "bronze"


def create_schema(engine) -> None:
    """Create the bronze schema if it doesn't exist."""
    with engine.connect() as conn:
        conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {BRONZE_SCHEMA}"))
        conn.commit()
    logger.info(f"Schema '{BRONZE_SCHEMA}' ready")


def load_csv_to_postgres(engine, csv_path: Path, table_name: str) -> None:
    """
    Load a single CSV into a PostgreSQL table.

    - Appends _ingested_at timestamp for lineage tracking
    - Uses replace strategy so reruns are idempotent
    - Logs row counts before and after for observability
    """
    if not csv_path.exists():
        logger.warning(f"File not found, skipping: {csv_path}")
        return

    logger.info(f"Loading {csv_path.name} → {BRONZE_SCHEMA}.{table_name}")

    df = pd.read_csv(csv_path, low_memory=False)
    row_count = len(df)

    # Add ingestion metadata columns - critical for lineage
    df["_ingested_at"] = datetime.now(timezone.utc).isoformat()
    df["_source_file"] = csv_path.name

    df.to_sql(
        name=table_name,
        con=engine,
        schema=BRONZE_SCHEMA,
        if_exists="replace",   # idempotent - safe to rerun
        index=False,
        chunksize=10_000,      # write in chunks for large files
        method="multi",
    )

    logger.success(f"  Loaded {row_count:,} rows → {BRONZE_SCHEMA}.{table_name}")


def run_ingestion() -> None:
    """Main ingestion entry point."""
    logger.info("=" * 60)
    logger.info("Olist Bronze Ingestion Starting")
    logger.info(f"Data directory: {DATA_DIR}")
    logger.info(f"Target database: {POSTGRES_DB} on {POSTGRES_HOST}")
    logger.info("=" * 60)

    engine = create_engine(DATABASE_URL)
    create_schema(engine)

    success_count = 0
    skip_count = 0

    for filename, table_name in OLIST_FILES.items():
        csv_path = DATA_DIR / filename
        try:
            load_csv_to_postgres(engine, csv_path, table_name)
            success_count += 1
        except Exception as e:
            logger.error(f"Failed to load {filename}: {e}")
            skip_count += 1

    logger.info("=" * 60)
    logger.info(f"Ingestion complete. Success: {success_count} | Skipped/Failed: {skip_count}")
    logger.info("=" * 60)


if __name__ == "__main__":
    run_ingestion()