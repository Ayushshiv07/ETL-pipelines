"""
======================================================
E-commerce ETL Pipeline — Apache Airflow DAG
======================================================
Orchestrates the ETL pipeline with daily scheduling.

Setup (on WSL/Linux):
    1. pip install apache-airflow
    2. airflow db init
    3. Copy this file to ~/airflow/dags/
    4. airflow webserver & airflow scheduler

Task flow:  generate → extract → transform → validate → load
======================================================
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

# ---- DAG Default Arguments ----
default_args = {
    "owner": "data_engineering",
    "depends_on_past": False,
    "email": ["alerts@yourcompany.com"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(hours=1),
}

# ---- DAG Definition ----
dag = DAG(
    dag_id="ecommerce_etl_pipeline",
    default_args=default_args,
    description="E-commerce ETL: Extract → Transform → Validate → Load",
    schedule_interval="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["etl", "ecommerce", "star-schema"],
)


# ---- Task Functions ----

def _extract(**context):
    """Extract raw data from CSVs."""
    import sys, os
    sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    from scripts.extract import extract_all

    raw_data = extract_all()
    # Log row counts to Airflow XCom
    row_counts = {k: len(v) for k, v in raw_data.items()}
    context["ti"].xcom_push(key="extract_row_counts", value=row_counts)
    return row_counts


def _transform(**context):
    """Transform raw data into star schema."""
    import sys, os
    sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    from scripts.extract import extract_all
    from scripts.transform import transform_all

    raw_data = extract_all()
    star_schema = transform_all(raw_data)
    row_counts = {k: len(v) for k, v in star_schema.items()}
    context["ti"].xcom_push(key="transform_row_counts", value=row_counts)
    return row_counts


def _validate(**context):
    """Run data quality checks."""
    import sys, os
    sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    from scripts.validate import validate_all

    results = validate_all()
    failed = [r for r in results if not r.passed]
    if failed:
        raise ValueError(f"Data validation failed: {len(failed)} checks failed")
    return {"checks_passed": len(results), "checks_failed": 0}


def _load(**context):
    """Load data into the data warehouse."""
    import sys, os
    sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    from scripts.load import load_all

    load_all(target="sqlite", mode="full")
    return {"status": "loaded"}


# ---- Task Definitions ----

extract_task = PythonOperator(
    task_id="extract_task",
    python_callable=_extract,
    dag=dag,
)

transform_task = PythonOperator(
    task_id="transform_task",
    python_callable=_transform,
    dag=dag,
)

validate_task = PythonOperator(
    task_id="validate_task",
    python_callable=_validate,
    dag=dag,
)

load_task = PythonOperator(
    task_id="load_task",
    python_callable=_load,
    dag=dag,
)

# ---- Task Dependencies ----
extract_task >> transform_task >> validate_task >> load_task
