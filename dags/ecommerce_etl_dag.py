"""
======================================================
E-Commerce ETL Pipeline — Apache Airflow DAG
======================================================
Production-grade orchestration with:
  - Daily scheduling for continuous reporting automation
  - XCom data passing between tasks
  - SLA monitoring + failure alerting
  - Row count validation gates
  - Resume claim: "Airflow DAG orchestration for
    continuous reporting automation"

Task Flow:
  extract → transform → validate → load → notify

Setup (WSL/Linux):
  1. pip install apache-airflow
  2. airflow db init
  3. Copy to ~/airflow/dags/
  4. airflow webserver & airflow scheduler
  5. Visit http://localhost:8080
======================================================
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.email import EmailOperator
from airflow.utils.dates import days_ago
import logging

logger = logging.getLogger(__name__)

# ── DAG Default Arguments ────────────────────────────────────────────────────
default_args = {
    "owner": "data_engineering",
    "depends_on_past": False,
    "email": ["alerts@yourcompany.com"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(hours=2),
}

# ── DAG Definition ───────────────────────────────────────────────────────────
dag = DAG(
    dag_id="ecommerce_etl_pipeline",
    default_args=default_args,
    description="E-Commerce ETL: 100K+ records → Star-Schema DWH → 8 Analytics Queries",
    schedule_interval="@daily",        # Continuous reporting automation
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["etl", "ecommerce", "star-schema", "production"],
    doc_md="""
    ## E-Commerce ETL Pipeline DAG

    **Processes 100,000+ records daily** through the following stages:

    1. **Extract** — Reads raw CSVs (orders, customers, products)
    2. **Transform** — Builds star-schema (fact_orders + 3 dimension tables)
    3. **Validate** — Runs 16 automated data quality checks
    4. **Load** — Writes to SQLite (dev) or BigQuery (prod)

    **Schedule:** Daily at midnight UTC  
    **SLA:** 30 minutes  
    **Owner:** Data Engineering Team
    """,
)


# ── Task: Extract ────────────────────────────────────────────────────────────
def task_extract(**context):
    """Extract raw data from CSV source files into DataFrames."""
    import sys, os
    sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    from scripts.extract import extract_all

    raw_data = extract_all()

    # Push row counts to XCom for downstream tasks
    row_counts = {k: len(v) for k, v in raw_data.items()}
    context["ti"].xcom_push(key="extract_row_counts", value=row_counts)
    total = sum(row_counts.values())

    logger.info(f"Extract complete: {total:,} total raw records")
    logger.info(f"Breakdown: {row_counts}")

    # Gate: fail if source files are empty
    if total == 0:
        raise ValueError("Extract returned 0 records — source files may be missing!")

    return row_counts


# ── Task: Transform ──────────────────────────────────────────────────────────
def task_transform(**context):
    """Clean raw data and build star schema (fact + 3 dimension tables)."""
    import sys, os
    sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    from scripts.extract import extract_all
    from scripts.transform import transform_all

    raw_data = extract_all()
    star_schema = transform_all(raw_data)

    row_counts = {k: len(v) for k, v in star_schema.items()}
    context["ti"].xcom_push(key="transform_row_counts", value=row_counts)

    fact_rows = row_counts.get("fact_orders", 0)
    logger.info(f"Transform complete: {fact_rows:,} fact records in star schema")

    # Gate: warn if significant data loss during cleaning (>10%)
    extract_counts = context["ti"].xcom_pull(key="extract_row_counts")
    if extract_counts:
        raw_orders = extract_counts.get("orders", 0)
        if raw_orders > 0 and fact_rows < raw_orders * 0.85:
            logger.warning(
                f"High data loss during transform: {raw_orders:,} raw → "
                f"{fact_rows:,} fact ({(1 - fact_rows/raw_orders)*100:.1f}% dropped)"
            )

    return row_counts


# ── Task: Validate ───────────────────────────────────────────────────────────
def task_validate(**context):
    """Run 16 automated data quality checks. Fails DAG if any check fails."""
    import sys, os
    sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    from scripts.extract import extract_all
    from scripts.transform import transform_all
    from scripts.validate import validate_all

    raw_data = extract_all()
    star_schema = transform_all(raw_data)
    results = validate_all(star_schema)

    passed = [r for r in results if r.passed]
    failed = [r for r in results if not r.passed]

    quality_score = int(len(passed) / len(results) * 100) if results else 0

    context["ti"].xcom_push(key="quality_score", value=quality_score)
    context["ti"].xcom_push(key="checks_passed", value=len(passed))
    context["ti"].xcom_push(key="checks_failed", value=len(failed))

    logger.info(f"Validation: {len(passed)}/{len(results)} checks passed (score: {quality_score}%)")

    if failed:
        failed_names = [f"{r.table}.{r.check}" for r in failed]
        raise ValueError(
            f"DATA QUALITY GATE FAILED: {len(failed)} checks failed → {failed_names}"
        )

    return {"quality_score": quality_score, "passed": len(passed), "failed": 0}


# ── Task: Load ───────────────────────────────────────────────────────────────
def task_load(**context):
    """Load validated star-schema tables into the data warehouse."""
    import sys, os
    sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    from scripts.extract import extract_all
    from scripts.transform import transform_all
    from scripts.load import load_all

    raw_data = extract_all()
    star_schema = transform_all(raw_data)

    # Support environment-based target selection
    target = os.getenv("DWH_TARGET", "sqlite")   # override with env var in prod
    mode   = os.getenv("LOAD_MODE",   "full")

    load_all(star_schema, target=target, mode=mode)

    total_loaded = sum(len(df) for df in star_schema.values())
    context["ti"].xcom_push(key="records_loaded", value=total_loaded)

    logger.info(f"Load complete: {total_loaded:,} records written to {target.upper()}")
    return {"target": target, "records_loaded": total_loaded}


# ── Task: Summary Notification ───────────────────────────────────────────────
def task_notify(**context):
    """Log pipeline execution summary (extendable to Slack/Teams/email)."""
    ti = context["ti"]
    exec_date = context["ds"]

    extract_counts  = ti.xcom_pull(task_ids="extract",  key="extract_row_counts")  or {}
    transform_counts= ti.xcom_pull(task_ids="transform",key="transform_row_counts") or {}
    quality_score   = ti.xcom_pull(task_ids="validate", key="quality_score")        or 0
    records_loaded  = ti.xcom_pull(task_ids="load",     key="records_loaded")       or 0

    summary = f"""
    ╔══════════════════════════════════════════════╗
    ║   E-COMMERCE ETL PIPELINE — DAILY SUMMARY   ║
    ╠══════════════════════════════════════════════╣
    ║  Execution Date : {exec_date}                ║
    ║  Raw Records    : {sum(extract_counts.values()):>10,}                ║
    ║  Fact Orders    : {transform_counts.get('fact_orders', 0):>10,}                ║
    ║  Quality Score  : {quality_score:>9}%                ║
    ║  Records Loaded : {records_loaded:>10,}                ║
    ║  Status         : ✅ SUCCESS                 ║
    ╚══════════════════════════════════════════════╝
    """
    logger.info(summary)

    # TODO: Extend with Slack webhook, Teams notification, or email
    # import requests
    # requests.post(SLACK_WEBHOOK, json={"text": summary})

    return {"status": "notified", "execution_date": exec_date}


# ── Task Instances ────────────────────────────────────────────────────────────
extract_op = PythonOperator(
    task_id="extract",
    python_callable=task_extract,
    dag=dag,
    sla=timedelta(minutes=10),
    doc_md="Reads orders.csv (100K+), customers.csv, products.csv from data/raw/",
)

transform_op = PythonOperator(
    task_id="transform",
    python_callable=task_transform,
    dag=dag,
    sla=timedelta(minutes=15),
    doc_md="Builds star schema: fact_orders + dim_customers + dim_products + dim_date",
)

validate_op = PythonOperator(
    task_id="validate",
    python_callable=task_validate,
    dag=dag,
    sla=timedelta(minutes=5),
    doc_md="Runs 16 quality checks (null PKs, duplicates, FK integrity, revenue > 0)",
)

load_op = PythonOperator(
    task_id="load",
    python_callable=task_load,
    dag=dag,
    sla=timedelta(minutes=10),
    doc_md="Loads star schema to SQLite (dev) or BigQuery (prod via DWH_TARGET env var)",
)

notify_op = PythonOperator(
    task_id="notify",
    python_callable=task_notify,
    dag=dag,
    trigger_rule="all_success",
    doc_md="Logs daily summary: records processed, quality score, load status",
)

# ── Task Dependencies ─────────────────────────────────────────────────────────
# extract → transform → validate → load → notify
extract_op >> transform_op >> validate_op >> load_op >> notify_op
