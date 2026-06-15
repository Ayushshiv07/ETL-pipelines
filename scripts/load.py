"""
======================================================
E-commerce ETL Pipeline — Load Layer
======================================================
Loads star schema tables into a data warehouse.
Supports: SQLite (local dev) and Google BigQuery (production).
Provides full replace loading and watermark-based incremental loading.

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


# ---- Watermark Helpers ----

def get_sqlite_watermark(engine) -> int:
    """Retrieve the maximum date_id loaded in the fact_orders SQLite table."""
    try:
        with engine.connect() as conn:
            val = conn.execute(text("SELECT MAX(date_id) FROM fact_orders")).scalar()
            return int(val) if val is not None else 0
    except Exception:
        return 0


def get_bigquery_watermark(client, dataset_ref) -> int:
    """Retrieve the maximum date_id loaded in the fact_orders BigQuery table."""
    table_id = f"{dataset_ref}.fact_orders"
    try:
        client.get_table(table_id)
        query = f"SELECT MAX(date_id) FROM `{table_id}`"
        query_job = client.query(query)
        result = query_job.result()
        for row in result:
            val = row[0]
            return int(val) if val is not None else 0
    except Exception:
        return 0


# ---- SQLite Loader ----

def load_to_sqlite(star_schema, mode="full"):
    """
    Load star schema tables into a local SQLite database.

    Args:
        star_schema: dict of table_name -> DataFrame
        mode: 'full' (replace) or 'incremental' (append with watermark filter)
    """
    db_path = os.path.join(PROJECT_ROOT, config["sqlite"]["database_path"])
    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    engine = create_engine(f"sqlite:///{db_path}")

    logger.info(f"Loading to SQLite ({mode} mode): {db_path}")

    if mode == "full":
        for table_name, df in star_schema.items():
            df.to_sql(table_name, engine, if_exists="replace", index=False)
            logger.info(f"  [OK] {table_name}: {len(df)} rows loaded")
    else:
        # Incremental mode
        watermark = get_sqlite_watermark(engine)
        logger.info(f"  Existing fact_orders watermark (MAX date_id): {watermark}")

        # Filter fact orders
        fact_df = star_schema["fact_orders"]
        new_fact_df = fact_df[fact_df["date_id"] > watermark]
        logger.info(f"  Filtered fact_orders: {len(new_fact_df)} of {len(fact_df)} rows are new (date_id > {watermark})")

        # Deduplicate and load dimension tables (upsert)
        for table_name in ["dim_customers", "dim_products", "dim_date"]:
            new_df = star_schema[table_name]
            existing_df = None
            try:
                existing_df = pd.read_sql_table(table_name, engine)
            except Exception:
                pass

            if existing_df is not None:
                pk = "customer_id" if table_name == "dim_customers" else ("product_id" if table_name == "dim_products" else "date_id")
                combined_df = pd.concat([existing_df, new_df]).drop_duplicates(subset=[pk], keep="last")
                combined_df.to_sql(table_name, engine, if_exists="replace", index=False)
                logger.info(f"  [OK] {table_name} (Upserted): {len(combined_df)} total rows ({len(combined_df) - len(existing_df)} new)")
            else:
                new_df.to_sql(table_name, engine, if_exists="replace", index=False)
                logger.info(f"  [OK] {table_name} (Created): {len(new_df)} rows")

        # Append new fact orders rows
        if len(new_fact_df) > 0:
            new_fact_df.to_sql("fact_orders", engine, if_exists="append", index=False)
            logger.info(f"  [OK] fact_orders (Appended): {len(new_fact_df)} rows loaded")
        else:
            logger.info("  [SKIP] No new fact_orders rows to load")

    # Verify row counts
    with engine.connect() as conn:
        logger.info("  Verification:")
        for table_name in star_schema:
            try:
                count = conn.execute(text(f"SELECT COUNT(*) FROM {table_name}")).scalar()
                logger.info(f"    {table_name}: {count} rows in DB")
            except Exception as e:
                logger.error(f"    Failed verification for {table_name}: {e}")

    logger.info("SQLite load complete [OK]")


# ---- BigQuery Loader ----

def load_to_bigquery(star_schema, mode="full"):
    """
    Load star schema tables into Google BigQuery.

    Args:
        star_schema: dict of table_name -> DataFrame
        mode: 'full' (WRITE_TRUNCATE) or 'incremental' (WRITE_APPEND with watermark filter)
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

    logger.info(f"Loading to BigQuery ({mode} mode)...")

    if mode == "full":
        for table_name, df in star_schema.items():
            table_id = f"{dataset_ref}.{table_name}"
            job_config = bigquery.LoadJobConfig(write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE)
            job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
            job.result()  # Wait for completion
            table = client.get_table(table_id)
            logger.info(f"  [OK] {table_name}: {table.num_rows} rows in BigQuery")
    else:
        # Incremental mode
        watermark = get_bigquery_watermark(client, dataset_ref)
        logger.info(f"  Existing fact_orders watermark (MAX date_id): {watermark}")

        # Filter fact orders
        fact_df = star_schema["fact_orders"]
        new_fact_df = fact_df[fact_df["date_id"] > watermark]
        logger.info(f"  Filtered fact_orders: {len(new_fact_df)} of {len(fact_df)} rows are new (date_id > {watermark})")

        # Deduplicate and load dimension tables (upsert via read-combine-replace)
        for table_name in ["dim_customers", "dim_products", "dim_date"]:
            new_df = star_schema[table_name]
            existing_df = None
            table_id = f"{dataset_ref}.{table_name}"
            try:
                client.get_table(table_id)
                query = f"SELECT * FROM `{table_id}`"
                existing_df = client.query(query).to_dataframe()
            except Exception:
                pass

            if existing_df is not None:
                pk = "customer_id" if table_name == "dim_customers" else ("product_id" if table_name == "dim_products" else "date_id")
                combined_df = pd.concat([existing_df, new_df]).drop_duplicates(subset=[pk], keep="last")
                job_config = bigquery.LoadJobConfig(write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE)
                job = client.load_table_from_dataframe(combined_df, table_id, job_config=job_config)
                job.result()
                logger.info(f"  [OK] {table_name} (Upserted): {len(combined_df)} rows in BigQuery")
            else:
                job_config = bigquery.LoadJobConfig(write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE)
                job = client.load_table_from_dataframe(new_df, table_id, job_config=job_config)
                job.result()
                logger.info(f"  [OK] {table_name} (Created): {len(new_df)} rows in BigQuery")

        # Append new fact orders rows
        if len(new_fact_df) > 0:
            table_id = f"{dataset_ref}.fact_orders"
            job_config = bigquery.LoadJobConfig(write_disposition=bigquery.WriteDisposition.WRITE_APPEND)
            job = client.load_table_from_dataframe(new_fact_df, table_id, job_config=job_config)
            job.result()
            table = client.get_table(table_id)
            logger.info(f"  [OK] fact_orders (Appended): {table.num_rows} total rows in BigQuery")
        else:
            logger.info("  [SKIP] No new fact_orders rows to load")

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
