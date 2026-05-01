"""
======================================================
E-commerce ETL Pipeline — Unit Tests
======================================================
Tests for extract, transform, validate, and load layers.

Usage:
    pytest tests/test_pipeline.py -v
======================================================
"""

import os, sys
import pytest
import pandas as pd

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, PROJECT_ROOT)

from scripts.extract import extract_csv, extract_all
from scripts.transform import (
    clean_orders, clean_customers, clean_products,
    build_dim_date, build_dim_customers, build_dim_products, build_fact_orders,
    transform_all,
)
from scripts.validate import (
    check_null_pks, check_duplicate_pks, check_referential_integrity,
    check_row_count, check_revenue_positive,
)


# ---- Fixtures ----

@pytest.fixture
def sample_orders():
    return pd.DataFrame({
        "order_id": [1, 2, 3, 3, 4, 5],           # duplicate id=3
        "customer_id": [10, 20, 10, 10, None, 30], # null customer
        "product_id": [100, 200, 100, 100, 300, 200],
        "quantity": [2, 1, 3, 3, 5, None],          # null quantity
        "order_date": ["2024-01-15", "2024-02-20", "2024-01-15",
                       "2024-01-15", "2024-03-10", "2024-04-05"],
        "status": ["completed", "shipped", "completed",
                   "completed", "pending", "cancelled"],
    })

@pytest.fixture
def sample_customers():
    return pd.DataFrame({
        "customer_id": [10, 20, 30, 30],  # duplicate id=30
        "name": ["Alice Smith", None, "Charlie Brown", "Charlie Brown"],
        "email": ["alice@test.com", "bob@test.com", None, None],
        "location": ["NYC, NY", "LA, CA", "Chicago, IL", "Chicago, IL"],
        "signup_date": ["2023-01-01", "2023-06-15", "2024-01-01", "2024-01-01"],
    })

@pytest.fixture
def sample_products():
    return pd.DataFrame({
        "product_id": [100, 200, 300],
        "product_name": ["Widget A", None, "Gadget C"],
        "category": ["Electronics", "Clothing", "Electronics"],
        "price": [29.99, 49.99, None],
        "supplier": ["Supplier X", "Supplier Y", "Supplier Z"],
    })


# ---- Extract Tests ----

class TestExtract:
    def test_extract_csv_file_not_found(self):
        with pytest.raises(FileNotFoundError):
            extract_csv("nonexistent_file.csv")

    def test_extract_csv_reads_data(self, tmp_path):
        csv_path = tmp_path / "test.csv"
        pd.DataFrame({"a": [1, 2], "b": [3, 4]}).to_csv(csv_path, index=False)
        df = extract_csv(str(csv_path))
        assert len(df) == 2
        assert list(df.columns) == ["a", "b"]


# ---- Transform / Cleaning Tests ----

class TestCleanOrders:
    def test_removes_null_keys(self, sample_orders):
        result = clean_orders(sample_orders)
        assert result["customer_id"].isnull().sum() == 0
        assert result["quantity"].isnull().sum() == 0

    def test_removes_duplicates(self, sample_orders):
        result = clean_orders(sample_orders)
        assert result["order_id"].duplicated().sum() == 0

    def test_fixes_dtypes(self, sample_orders):
        result = clean_orders(sample_orders)
        assert result["order_id"].dtype == int
        assert pd.api.types.is_datetime64_any_dtype(result["order_date"])

    def test_row_count_decreases(self, sample_orders):
        result = clean_orders(sample_orders)
        assert len(result) < len(sample_orders)


class TestCleanCustomers:
    def test_removes_duplicates(self, sample_customers):
        result = clean_customers(sample_customers)
        assert result["customer_id"].duplicated().sum() == 0

    def test_fills_null_names(self, sample_customers):
        result = clean_customers(sample_customers)
        assert result["name"].isnull().sum() == 0

    def test_standardizes_email(self, sample_customers):
        result = clean_customers(sample_customers)
        for email in result["email"]:
            assert email == email.lower()


class TestCleanProducts:
    def test_fills_null_prices(self, sample_products):
        result = clean_products(sample_products)
        assert result["price"].isnull().sum() == 0

    def test_fills_null_names(self, sample_products):
        result = clean_products(sample_products)
        assert result["product_name"].isnull().sum() == 0


# ---- Star Schema Builder Tests ----

class TestStarSchema:
    def test_dim_date_has_required_columns(self, sample_orders):
        orders = clean_orders(sample_orders)
        dim = build_dim_date(orders)
        required = ["date_id", "full_date", "day", "month", "year", "quarter"]
        for col in required:
            assert col in dim.columns

    def test_dim_date_unique_dates(self, sample_orders):
        orders = clean_orders(sample_orders)
        dim = build_dim_date(orders)
        assert dim["date_id"].duplicated().sum() == 0

    def test_fact_orders_has_revenue(self, sample_orders, sample_products):
        orders = clean_orders(sample_orders)
        products = clean_products(sample_products)
        fact = build_fact_orders(orders, products)
        assert "revenue" in fact.columns
        assert (fact["revenue"] >= 0).all()

    def test_fact_orders_revenue_formula(self, sample_orders, sample_products):
        orders = clean_orders(sample_orders)
        products = clean_products(sample_products)
        fact = build_fact_orders(orders, products)
        expected = (fact["quantity"] * fact["unit_price"]).round(2)
        pd.testing.assert_series_equal(fact["revenue"], expected, check_names=False)


# ---- Validation Tests ----

class TestValidation:
    def test_null_pk_pass(self):
        df = pd.DataFrame({"id": [1, 2, 3]})
        result = check_null_pks(df, "test", "id")
        assert result.passed

    def test_null_pk_fail(self):
        df = pd.DataFrame({"id": [1, None, 3]})
        result = check_null_pks(df, "test", "id")
        assert not result.passed

    def test_duplicate_pk_pass(self):
        df = pd.DataFrame({"id": [1, 2, 3]})
        result = check_duplicate_pks(df, "test", "id")
        assert result.passed

    def test_duplicate_pk_fail(self):
        df = pd.DataFrame({"id": [1, 2, 2]})
        result = check_duplicate_pks(df, "test", "id")
        assert not result.passed

    def test_referential_integrity_pass(self):
        fact = pd.DataFrame({"fk": [1, 2]})
        dim = pd.DataFrame({"fk": [1, 2, 3]})
        result = check_referential_integrity(fact, dim, "fk", "dim")
        assert result.passed

    def test_referential_integrity_fail(self):
        fact = pd.DataFrame({"fk": [1, 2, 99]})
        dim = pd.DataFrame({"fk": [1, 2]})
        result = check_referential_integrity(fact, dim, "fk", "dim")
        assert not result.passed

    def test_revenue_positive(self):
        df = pd.DataFrame({"revenue": [10, 20, 30]})
        result = check_revenue_positive(df)
        assert result.passed


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
