"""Quick analytics runner - queries the SQLite warehouse."""
import sqlite3, os

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
db = os.path.join(PROJECT_ROOT, "data", "ecommerce_dwh.db")
conn = sqlite3.connect(db)
c = conn.cursor()

print("=" * 60)
print("TABLE ROW COUNTS")
print("=" * 60)
for t in ["fact_orders", "dim_customers", "dim_products", "dim_date"]:
    count = c.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0]
    print(f"  {t}: {count:,} rows")

print("\n" + "=" * 60)
print("TOP 5 REVENUE MONTHS")
print("=" * 60)
rows = c.execute("""
    SELECT d.year, d.month, d.month_name,
           COUNT(DISTINCT f.order_id) AS orders,
           ROUND(SUM(f.revenue), 2) AS revenue
    FROM fact_orders f
    JOIN dim_date d ON f.date_id = d.date_id
    WHERE f.order_status != 'cancelled'
    GROUP BY d.year, d.month
    ORDER BY revenue DESC LIMIT 5
""").fetchall()
for r in rows:
    print(f"  {r[0]}-{r[1]:02d} ({r[2]:>9s}): {r[3]:>4} orders, ${r[4]:>12,.2f}")

print("\n" + "=" * 60)
print("TOP 10 PRODUCTS BY REVENUE")
print("=" * 60)
rows = c.execute("""
    SELECT p.product_name, p.category, ROUND(SUM(f.revenue), 2) AS rev
    FROM fact_orders f
    JOIN dim_products p ON f.product_id = p.product_id
    WHERE f.order_status != 'cancelled'
    GROUP BY p.product_id
    ORDER BY rev DESC LIMIT 10
""").fetchall()
for i, r in enumerate(rows, 1):
    print(f"  {i:>2}. {r[0]:<35s} ({r[1]:<18s}) ${r[2]:>10,.2f}")

print("\n" + "=" * 60)
print("TOP 5 CUSTOMERS BY LIFETIME VALUE")
print("=" * 60)
rows = c.execute("""
    SELECT c.name, c.location, COUNT(DISTINCT f.order_id) AS orders,
           ROUND(SUM(f.revenue), 2) AS clv
    FROM fact_orders f
    JOIN dim_customers c ON f.customer_id = c.customer_id
    WHERE f.order_status != 'cancelled'
    GROUP BY c.customer_id
    ORDER BY clv DESC LIMIT 5
""").fetchall()
for r in rows:
    print(f"  {r[0]:<25s} ({r[1]:<20s}) {r[2]:>3} orders, ${r[3]:>10,.2f} CLV")

print("\n" + "=" * 60)
print("REVENUE BY CATEGORY")
print("=" * 60)
rows = c.execute("""
    SELECT p.category, COUNT(DISTINCT f.order_id) AS orders,
           ROUND(SUM(f.revenue), 2) AS revenue
    FROM fact_orders f
    JOIN dim_products p ON f.product_id = p.product_id
    WHERE f.order_status != 'cancelled'
    GROUP BY p.category
    ORDER BY revenue DESC
""").fetchall()
for r in rows:
    print(f"  {r[0]:<20s}: {r[1]:>4} orders, ${r[2]:>12,.2f}")

print("\n" + "=" * 60)
print("ORDER STATUS BREAKDOWN")
print("=" * 60)
rows = c.execute("""
    SELECT order_status, COUNT(*) AS cnt, ROUND(SUM(revenue), 2) AS rev
    FROM fact_orders GROUP BY order_status ORDER BY cnt DESC
""").fetchall()
for r in rows:
    print(f"  {r[0]:<12s}: {r[1]:>5} orders, ${r[2]:>12,.2f}")

conn.close()
