"""
======================================================
E-commerce ETL Pipeline — Load Layer
======================================================
Loads star schema tables into a data warehouse.
Supports: SQLite (local dev) and Google BigQuery (production).

Usage:
    from scripts.load import load_all
    load_all(star_schema_dict, target="sqlite", mode="full")
======================================================
"""

import os, sys, logging
import pandas as pd
import yaml
from sqlalchemy import create_engine, text

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, PROJECT_ROOT)

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

with open(os.path.join(PROJECT_ROOT, "config", "pipeline_config.yaml"), "r") as f:
    config = yaml.safe_load(f)

# ---- SQLite Loader ----

def load_to_sqlite(star_schema, mode="full"):
    """
    Load star schema tables into a local SQLite database.

    Args:
        star_schema: dict of table_name -> DataFrame
        mode: 'full' (replace) or 'incremental' (append)
    """
    db_path = os.path.join(PROJECT_ROOT, config["sqlite"]["database_path"])
    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    engine = create_engine(f"sqlite:///{db_path}")

    if_exists = "replace" if mode == "full" else "append"

    logger.info(f"Loading to SQLite ({mode} mode): {db_path}")

    for table_name, df in star_schema.items():
        df.to_sql(table_name, engine, if_exists=if_exists, index=False)
        logger.info(f"  [OK] {table_name}: {len(df)} rows loaded")

    # Verify row counts
    with engine.connect() as conn:
        logger.info("  Verification:")
        for table_name in star_schema:
            count = conn.execute(text(f"SELECT COUNT(*) FROM {table_name}")).scalar()
            logger.info(f"    {table_name}: {count} rows in DB")

    logger.info("SQLite load complete [OK]")


# ---- BigQuery Loader ----

def load_to_bigquery(star_schema, mode="full"):
    """
    Load star schema tables into Google BigQuery.

    Requires:
      - google-cloud-bigquery installed
      - Service account JSON key at config path
      - BigQuery API enabled in GCP project

    Args:
        star_schema: dict of table_name -> DataFrame
        mode: 'full' (WRITE_TRUNCATE) or 'incremental' (WRITE_APPEND)
    """
    try:
        from google.cloud import bigquery
    except ImportError:
        logger.error("google-cloud-bigquery not installed. Run: pip install google-cloud-bigquery")
        raise

    bq_cfg = config["bigquery"]
    cred_path = os.path.join(PROJECT_ROOT, bq_cfg["credentials_path"])

    if not os.path.exists(cred_path):
        logger.error(f"BigQuery credentials not found: {cred_path}")
        logger.info("Set up a service account key at: https://console.cloud.google.com/iam-admin/serviceaccounts")
        raise FileNotFoundError(f"Missing credentials: {cred_path}")

    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = cred_path
    client = bigquery.Client(project=bq_cfg["project_id"])

    # Create dataset if it doesn't exist
    dataset_ref = f"{bq_cfg['project_id']}.{bq_cfg['dataset_id']}"
    dataset = bigquery.Dataset(dataset_ref)
    dataset.location = bq_cfg["location"]
    try:
        client.create_dataset(dataset, exists_ok=True)
        logger.info(f"Dataset ready: {dataset_ref}")
    except Exception as e:
        logger.error(f"Failed to create dataset: {e}")
        raise

    # Load tables
    write_disposition = (
        bigquery.WriteDisposition.WRITE_TRUNCATE if mode == "full"
        else bigquery.WriteDisposition.WRITE_APPEND
    )

    logger.info(f"Loading to BigQuery ({mode} mode)...")

    for table_name, df in star_schema.items():
        table_id = f"{dataset_ref}.{table_name}"
        job_config = bigquery.LoadJobConfig(write_disposition=write_disposition)

        job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
        job.result()  # Wait for completion

        table = client.get_table(table_id)
        logger.info(f"  [OK] {table_name}: {table.num_rows} rows in BigQuery")

    logger.info("BigQuery load complete [OK]")


# ---- Main Loader ----

def load_all(star_schema=None, target=None, mode=None):
    """
    Load star schema into the configured target warehouse.

    If star_schema is None, reads from transformed CSVs.
    """
    target = target or config["load"]["target"]
    mode = mode or config["load"]["mode"]

    logger.info("=" * 60)
    logger.info(f"LOAD LAYER — Target: {target} | Mode: {mode}")
    logger.info("=" * 60)

    # If no data passed, read from transformed CSVs
    if star_schema is None:
        t_dir = os.path.join(PROJECT_ROOT, config["paths"]["transformed_data"])
        star_schema = {}
        for tbl in ["fact_orders", "dim_customers", "dim_products", "dim_date"]:
            path = os.path.join(t_dir, f"{tbl}.csv")
            star_schema[tbl] = pd.read_csv(path)
            logger.info(f"  Read {tbl}: {len(star_schema[tbl])} rows from CSV")

    if target == "sqlite":
        load_to_sqlite(star_schema, mode)
    elif target == "bigquery":
        load_to_bigquery(star_schema, mode)
    else:
        raise ValueError(f"Unknown target: {target}. Use 'sqlite' or 'bigquery'.")


if __name__ == "__main__":
    load_all()
