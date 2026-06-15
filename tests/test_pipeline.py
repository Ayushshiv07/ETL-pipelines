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


class TestLoad:
    def test_sqlite_load_full_and_incremental(self, tmp_path):
        import time
        import scripts.load as load_module
        from scripts.load import load_to_sqlite
        from sqlalchemy import create_engine, text

        
        test_db_filename = f"test_{int(time.time())}.db"
        test_db_dir = tmp_path / "data"
        test_db_dir.mkdir(parents=True, exist_ok=True)
        test_db_path = test_db_dir / test_db_filename
        
        # Save original db path and mock it
        original_db_path = load_module.config["sqlite"]["database_path"]
        load_module.config["sqlite"]["database_path"] = str(test_db_path)
        
        try:
            # First batch (Full load)
            star_schema_1 = {
                "fact_orders": pd.DataFrame({
                    "order_id": [1, 2],
                    "customer_id": [10, 20],
                    "product_id": [100, 200],
                    "date_id": [20240101, 20240102],
                    "revenue": [100.0, 200.0],
                    "order_status": ["completed", "completed"]
                }),
                "dim_customers": pd.DataFrame({
                    "customer_id": [10, 20],
                    "name": ["Alice", "Bob"],
                    "email": ["alice@test.com", "bob@test.com"],
                    "location": ["NYC", "LA"]
                }),
                "dim_products": pd.DataFrame({
                    "product_id": [100, 200],
                    "product_name": ["Widget A", "Widget B"],
                    "category": ["Electronics", "Electronics"],
                    "price": [100.0, 200.0]
                }),
                "dim_date": pd.DataFrame({
                    "date_id": [20240101, 20240102],
                    "full_date": ["2024-01-01", "2024-01-02"],
                    "day": [1, 2],
                    "month": [1, 1],
                    "year": [2024, 2024],
                    "quarter": [1, 1]
                })
            }
            
            load_to_sqlite(star_schema_1, mode="full")
            
            # Verify full load
            engine = create_engine(f"sqlite:///{test_db_path}")
            with engine.connect() as conn:
                orders_count = conn.execute(text("SELECT COUNT(*) FROM fact_orders")).scalar()
                cust_count = conn.execute(text("SELECT COUNT(*) FROM dim_customers")).scalar()
                assert orders_count == 2
                assert cust_count == 2
                
                # Check data value
                bob_name = conn.execute(text("SELECT name FROM dim_customers WHERE customer_id = 20")).scalar()
                assert bob_name == "Bob"
            
            # Second batch (Incremental load)
            star_schema_2 = {
                "fact_orders": pd.DataFrame({
                    "order_id": [2, 3],  # 2 is old (date_id=20240102 <= watermark), 3 is new (date_id=20240103 > watermark)
                    "customer_id": [20, 30],
                    "product_id": [200, 300],
                    "date_id": [20240102, 20240103],
                    "revenue": [220.0, 300.0],
                    "order_status": ["completed", "pending"]
                }),
                "dim_customers": pd.DataFrame({
                    "customer_id": [20, 30],
                    "name": ["Bob Updated", "Charlie"],
                    "email": ["bob.updated@test.com", "charlie@test.com"],
                    "location": ["SF", "Chicago"]
                }),
                "dim_products": pd.DataFrame({
                    "product_id": [200, 300],
                    "product_name": ["Widget B Updated", "Widget C"],
                    "category": ["Electronics", "Clothing"],
                    "price": [220.0, 300.0]
                }),
                "dim_date": pd.DataFrame({
                    "date_id": [20240102, 20240103],
                    "full_date": ["2024-01-02", "2024-01-03"],
                    "day": [2, 3],
                    "month": [1, 1],
                    "year": [2024, 2024],
                    "quarter": [1, 1]
                })
            }
            
            load_to_sqlite(star_schema_2, mode="incremental")
            
            # Verify incremental load results
            with engine.connect() as conn:
                # fact_orders should have 3 rows total (appended order 3, filtered out order 2)
                orders_count = conn.execute(text("SELECT COUNT(*) FROM fact_orders")).scalar()
                assert orders_count == 3
                
                # order 3 exists, order 2 revenue is still 200.0 (not overwritten because it was filtered)
                ord2_rev = conn.execute(text("SELECT revenue FROM fact_orders WHERE order_id = 2")).scalar()
                assert ord2_rev == 200.0
                ord3_rev = conn.execute(text("SELECT revenue FROM fact_orders WHERE order_id = 3")).scalar()
                assert ord3_rev == 300.0
                
                # dim_customers should have 3 rows total, Bob should be updated to "Bob Updated"
                cust_count = conn.execute(text("SELECT COUNT(*) FROM dim_customers")).scalar()
                assert cust_count == 3
                bob_name = conn.execute(text("SELECT name FROM dim_customers WHERE customer_id = 20")).scalar()
                assert bob_name == "Bob Updated"
                charlie_name = conn.execute(text("SELECT name FROM dim_customers WHERE customer_id = 30")).scalar()
                assert charlie_name == "Charlie"
                
                # dim_products should have 3 rows total, Widget B should be updated
                prod_count = conn.execute(text("SELECT COUNT(*) FROM dim_products")).scalar()
                assert prod_count == 3
                prod2_name = conn.execute(text("SELECT product_name FROM dim_products WHERE product_id = 200")).scalar()
                assert prod2_name == "Widget B Updated"
                
        finally:
            # Restore original configuration
            load_module.config["sqlite"]["database_path"] = original_db_path


if __name__ == "__main__":
    pytest.main([__file__, "-v"])

