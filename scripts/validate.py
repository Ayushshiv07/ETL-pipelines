"""
======================================================
E-commerce ETL Pipeline — Data Validation
======================================================
Post-transform data quality checks:
  - No null primary keys
  - No duplicate primary keys
  - Row count consistency
  - Referential integrity (FKs exist in dimension tables)
  - Data type verification

Usage:
    from scripts.validate import validate_all
    report = validate_all(star_schema_dict)
======================================================
"""

import os, sys, logging
import pandas as pd
import yaml

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, PROJECT_ROOT)

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

with open(os.path.join(PROJECT_ROOT, "config", "pipeline_config.yaml"), "r") as f:
    config = yaml.safe_load(f)


class ValidationResult:
    """Holds results of a single validation check."""
    def __init__(self, check_name, table, passed, details=""):
        self.check_name = check_name
        self.table = table
        self.passed = passed
        self.details = details

    def __repr__(self):
        status = "PASS" if self.passed else "FAIL"
        return f"  [{status}] {self.table}.{self.check_name}: {self.details}"


def check_null_pks(df, table_name, pk_column):
    """Check that primary key column has no nulls."""
    null_count = df[pk_column].isnull().sum()
    passed = null_count == 0
    return ValidationResult(
        f"null_pk({pk_column})", table_name, passed,
        f"{null_count} nulls found" if not passed else "No nulls"
    )


def check_duplicate_pks(df, table_name, pk_column):
    """Check that primary key column has no duplicates."""
    dupe_count = df[pk_column].duplicated().sum()
    passed = dupe_count == 0
    return ValidationResult(
        f"duplicate_pk({pk_column})", table_name, passed,
        f"{dupe_count} duplicates found" if not passed else "No duplicates"
    )


def check_referential_integrity(fact_df, dim_df, fk_column, dim_table_name):
    """Check that all FK values in fact table exist in dimension table."""
    fact_keys = set(fact_df[fk_column].unique())
    dim_keys = set(dim_df[fk_column].unique())
    orphans = fact_keys - dim_keys
    passed = len(orphans) == 0
    return ValidationResult(
        f"ref_integrity({fk_column})", "fact_orders->" + dim_table_name, passed,
        f"{len(orphans)} orphan keys" if not passed else "All FKs valid"
    )


def check_row_count(df, table_name, min_rows=1):
    """Check that table has at least min_rows."""
    passed = len(df) >= min_rows
    return ValidationResult(
        "row_count", table_name, passed,
        f"{len(df)} rows (min: {min_rows})"
    )


def check_revenue_positive(fact_df):
    """Check that all revenue values are non-negative."""
    neg_count = (fact_df["revenue"] < 0).sum()
    passed = neg_count == 0
    return ValidationResult(
        "revenue_positive", "fact_orders", passed,
        f"{neg_count} negative values" if not passed else "All positive"
    )


def validate_all(star_schema=None):
    """
    Run all validation checks on the star schema.

    Returns:
        list of ValidationResult objects
    """
    logger.info("=" * 60)
    logger.info("VALIDATION LAYER — Running data quality checks")
    logger.info("=" * 60)

    # Load from CSV if not provided
    if star_schema is None:
        t_dir = os.path.join(PROJECT_ROOT, config["paths"]["transformed_data"])
        star_schema = {}
        for tbl in ["fact_orders", "dim_customers", "dim_products", "dim_date"]:
            star_schema[tbl] = pd.read_csv(os.path.join(t_dir, f"{tbl}.csv"))

    fact = star_schema["fact_orders"]
    dim_cust = star_schema["dim_customers"]
    dim_prod = star_schema["dim_products"]
    dim_date = star_schema["dim_date"]

    results = []

    # -- Null PK checks --
    results.append(check_null_pks(fact, "fact_orders", "order_id"))
    results.append(check_null_pks(dim_cust, "dim_customers", "customer_id"))
    results.append(check_null_pks(dim_prod, "dim_products", "product_id"))
    results.append(check_null_pks(dim_date, "dim_date", "date_id"))

    # -- Duplicate PK checks --
    results.append(check_duplicate_pks(fact, "fact_orders", "order_id"))
    results.append(check_duplicate_pks(dim_cust, "dim_customers", "customer_id"))
    results.append(check_duplicate_pks(dim_prod, "dim_products", "product_id"))
    results.append(check_duplicate_pks(dim_date, "dim_date", "date_id"))

    # -- Referential integrity --
    results.append(check_referential_integrity(fact, dim_cust, "customer_id", "dim_customers"))
    results.append(check_referential_integrity(fact, dim_prod, "product_id", "dim_products"))
    results.append(check_referential_integrity(fact, dim_date, "date_id", "dim_date"))

    # -- Row count checks --
    results.append(check_row_count(fact, "fact_orders", min_rows=100))
    results.append(check_row_count(dim_cust, "dim_customers", min_rows=10))
    results.append(check_row_count(dim_prod, "dim_products", min_rows=5))
    results.append(check_row_count(dim_date, "dim_date", min_rows=10))

    # -- Business rule checks --
    results.append(check_revenue_positive(fact))

    # -- Report --
    passed = sum(1 for r in results if r.passed)
    failed = sum(1 for r in results if not r.passed)

    logger.info("-" * 60)
    for r in results:
        logger.info(str(r))
    logger.info("-" * 60)
    logger.info(f"Results: {passed} passed, {failed} failed out of {len(results)} checks")

    if failed > 0:
        logger.warning("[WARN] VALIDATION FAILED — Review issues above")
    else:
        logger.info("[OK] ALL VALIDATIONS PASSED")

    # Save report
    report_dir = os.path.join(PROJECT_ROOT, config["paths"]["validated_data"])
    os.makedirs(report_dir, exist_ok=True)
    report_df = pd.DataFrame([{
        "check": r.check_name, "table": r.table,
        "passed": r.passed, "details": r.details
    } for r in results])
    report_path = os.path.join(report_dir, "validation_report.csv")
    report_df.to_csv(report_path, index=False)
    logger.info(f"  -> Report saved: {report_path}")

    return results


if __name__ == "__main__":
    results = validate_all()
    all_passed = all(r.passed for r in results)
    sys.exit(0 if all_passed else 1)
