# 🛒 E-Commerce ETL Pipeline

A production-level ETL pipeline that extracts raw e-commerce data (orders, customers, products), transforms it into a **star schema**, loads it into a data warehouse, and generates business insights.

---

## 📐 Architecture

```
┌──────────────┐    ┌──────────────────┐    ┌──────────────┐    ┌──────────────┐
│   Raw CSVs   │───▶│   Extract Layer  │───▶│  Transform   │───▶│  Validate    │
│ orders.csv   │    │   extract.py     │    │ transform.py │    │ validate.py  │
│ customers.csv│    │                  │    │              │    │              │
│ products.csv │    └──────────────────┘    └──────────────┘    └──────┬───────┘
└──────────────┘                                                      │
                                                                      ▼
                    ┌──────────────────┐    ┌──────────────┐    ┌──────────────┐
                    │   Dashboard      │◀───│  Analytics   │◀───│  Load Layer  │
                    │  dashboard.py    │    │  SQL Queries  │    │   load.py    │
                    └──────────────────┘    └──────────────┘    │ SQLite / BQ  │
                                                                └──────────────┘
```

### Star Schema

```
              ┌───────────────┐
              │  dim_customers│
              │───────────────│
              │ customer_id PK│
              │ name          │
              │ email         │
              │ location      │
              └───────┬───────┘
                      │
┌───────────────┐     │     ┌───────────────┐
│  dim_products │     │     │   dim_date    │
│───────────────│     │     │───────────────│
│ product_id PK │     │     │ date_id    PK │
│ product_name  │     │     │ full_date     │
│ category      │◀────┼────▶│ month / year  │
│ price         │     │     │ quarter       │
│ supplier      │     │     │ day_of_week   │
└───────────────┘     │     └───────────────┘
                      │
              ┌───────┴───────┐
              │  fact_orders  │
              │───────────────│
              │ order_id   PK │
              │ customer_id FK│
              │ product_id FK │
              │ date_id    FK │
              │ quantity      │
              │ unit_price    │
              │ revenue       │
              │ order_status  │
              └───────────────┘
```

---

## 🧰 Tech Stack

| Component       | Technology                          |
|----------------|--------------------------------------|
| Language        | Python 3.10+                        |
| Data Processing | Pandas, SQLAlchemy                  |
| Warehouse (Dev) | SQLite                              |
| Warehouse (Prod)| Google BigQuery                     |
| Orchestration   | Apache Airflow / Standalone Runner  |
| Visualization   | Plotly (interactive HTML)           |
| Testing         | pytest                              |

---

## 📁 Project Structure

```
ETL pipelines/
├── config/
│   └── pipeline_config.yaml    # Central configuration
├── dags/
│   └── ecommerce_etl_dag.py    # Airflow DAG
├── data/
│   ├── raw/                    # Generated source CSVs
│   ├── transformed/            # Star schema CSVs
│   └── validated/              # Validation reports + dashboard
├── logs/                       # Pipeline execution logs
├── scripts/
│   ├── generate_data.py        # Fake data generator
│   ├── extract.py              # Extract layer
│   ├── transform.py            # Transform layer
│   ├── load.py                 # Load layer (SQLite + BigQuery)
│   ├── validate.py             # Data validation
│   ├── run_pipeline.py         # Standalone pipeline runner
│   └── dashboard.py            # Plotly dashboard generator
├── sql/
│   ├── create_tables.sql       # Star schema DDL
│   └── analytics_queries.sql   # Business insight queries
├── tests/
│   └── test_pipeline.py        # Unit tests
├── requirements.txt
└── README.md
```

---

## 🚀 Quick Start

### 1. Create Virtual Environment

```bash
cd "ETL pipelines"
python -m venv venv
venv\Scripts\activate          # Windows
# source venv/bin/activate     # macOS/Linux
```

### 2. Install Dependencies

```bash
pip install -r requirements.txt
```

### 3. Generate Sample Data

```bash
python scripts/generate_data.py
```

### 4. Run Full Pipeline

```bash
# Full pipeline: Extract → Transform → Validate → Load (SQLite)
python scripts/run_pipeline.py --mode full --generate

# Without regenerating data
python scripts/run_pipeline.py --mode full
```

### 5. Generate Dashboard

```bash
python scripts/dashboard.py
# Opens data/validated/dashboard.html in your browser
```

### 6. Run Tests

```bash
pytest tests/ -v
```

---

## ⚙️ Configuration

All settings are in `config/pipeline_config.yaml`:

- **Data generation**: Number of records, null/duplicate rates
- **File paths**: Input/output directories
- **Load target**: `sqlite` (default) or `bigquery`
- **Load mode**: `full` (replace) or `incremental` (append)

---

## ☁️ BigQuery Setup (Optional)

1. Create a GCP project and enable the BigQuery API
2. Create a service account with BigQuery Admin role
3. Download the JSON key file
4. Place it at `config/bigquery_credentials.json`
5. Update `config/pipeline_config.yaml`:

```yaml
bigquery:
  project_id: "your-project-id"
  dataset_id: "ecommerce_dwh"
load:
  target: "bigquery"
```

6. Run:
```bash
python scripts/run_pipeline.py --target bigquery --mode full
```

---

## 🔄 Airflow Setup (WSL/Linux)

Apache Airflow requires Linux. On Windows, use WSL:

```bash
# In WSL terminal
pip install apache-airflow
airflow db init
airflow users create --username admin --password admin --role Admin \
    --firstname Admin --lastname User --email admin@example.com

# Copy the DAG
cp dags/ecommerce_etl_dag.py ~/airflow/dags/

# Start services
airflow webserver -p 8080 &
airflow scheduler &
```

Visit `http://localhost:8080` to see the DAG.

---

## 📊 Analytics Queries

The `sql/analytics_queries.sql` file includes 8 production queries:

1. **Monthly Revenue Trend** — Revenue by year-month
2. **Top 10 Products** — Best sellers by revenue
3. **Customer Lifetime Value** — Total spend per customer
4. **Repeat vs New Customers** — Monthly cohort analysis
5. **Revenue by Category** — Category performance
6. **Average Order Value** — AOV trend over time
7. **Order Status Breakdown** — Completion rates
8. **Day-of-Week Analysis** — Best performing days

---

## ✅ Data Validation

The pipeline runs 16 automated checks:

| Check                  | Description                              |
|------------------------|------------------------------------------|
| Null primary keys      | No nulls in PK columns                  |
| Duplicate primary keys | No duplicate PKs                         |
| Referential integrity  | All FKs exist in dimension tables        |
| Row count              | Tables have minimum expected rows        |
| Revenue positive       | No negative revenue values               |

---

## 🌐 Deployment

You can deploy this dashboard to various platforms:

### 1. Streamlit Community Cloud (Easiest)
1. Push this repository to **GitHub**.
2. Connect your GitHub account to [Streamlit Cloud](https://share.streamlit.io/).
3. Select this repo and `app.py` as the main file.
4. Your app will be live at a public URL!

### 2. Docker
Build and run the container locally or on any cloud provider:
```bash
docker build -t etl-dashboard .
docker run -p 8501:8501 etl-dashboard
```

### 3. Heroku
The included `Procfile` makes it ready for Heroku:
1. Create a Heroku app.
2. Push your code: `git push heroku main`.

---

## 📝 License

This project is for educational and portfolio purposes.
