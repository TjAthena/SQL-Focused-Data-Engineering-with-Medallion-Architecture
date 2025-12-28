# SQL-Focused Data Engineering with Medallion Architecture

A compact, hands-on learning project for SQL practitioners and data engineers. Use realistic CSV data to practice ETL, cleaning, schema design (bronze/silver/gold), and query performance optimization without introducing heavy orchestration frameworks.

## Key Goals
- **Practice:** SQL at scale (~1.55M rows)
- **Learn:** Medallion architecture (Bronze → Silver → Gold)
- **Optimize:** Joins, aggregations, indexing, and execution plans
- **Simplify:** Minimal toolchain so learners focus on SQL fundamentals

## Datasets (raw)
All raw CSVs are in the `olist_raw_locall_view` folder. Important files:
- `olist_orders_dataset.csv` — order-level data and timestamps
- `olist_order_items_dataset.csv` — items per order (product, seller)
- `olist_products_dataset.csv` — product metadata
- `olist_customers_dataset.csv` — customer addresses & IDs
- `olist_order_payments_dataset.csv` — payment methods and amounts
- `olist_order_reviews_dataset.csv` — customer reviews
- `olist_sellers_dataset.csv` — seller metadata
- `olist_geolocation_dataset.csv` — location lookups
- `product_category_name_translation.csv` — product category translations

## Repo Structure
- **`olist_raw_locall_view/`**: Raw CSVs (listed above)
- **`Scripts/Loading_script/`**: ingestion helpers (e.g., `customers_table_load.py`)
- **`Scripts/Transfermation_scripts/`**: SQL transformation scripts for Silver/Gold layers
- **`README.md`**: this file

Files of interest:
- [Scripts/Loading_script/customers_table_load.py](Scripts/Loading_script/customers_table_load.py)
- [Scripts/Transfermation_scripts/silver_schema_tranfermation.sql](Scripts/Transfermation_scripts/silver_schema_tranfermation.sql)
- [Scripts/Transfermation_scripts/gold_schema_transformations.sql](Scripts/Transfermation_scripts/gold_schema_transformations.sql)

## Quickstart
Prerequisites: Python 3.8+, and optionally PostgreSQL or DuckDB for querying.

1. Inspect raw data (CSV) in `olist_raw_locall_view/`.
2. To run the sample loader for customers (example):

```bash
python Scripts/Loading_script/customers_table_load.py
```

3. Use the SQL scripts in `Scripts/Transfermation_scripts/` to build Silver/Gold tables (adapt paths and DB connection as needed). Example using `psql` for PostgreSQL:

```bash
# Run silver transformations
psql -d mydb -f Scripts/Transfermation_scripts/silver_schema_tranfermation.sql

# Run gold transformations
psql -d mydb -f Scripts/Transfermation_scripts/gold_schema_transformations.sql
```

Alternatively, export Gold tables as CSV and query locally with DuckDB or load into your analytics DB.

## Recommended Learning Exercises
- Compare query times before/after adding indexes
- Refactor slow joins into pre-aggregated gold tables
- Explore execution plans for heavy aggregations
- Test denormalized vs normalized query performance

## How to Share Gold Data (three options)
- **Commit CSVs to repo** (`/data/gold/`) — easiest for learners
- **Provide SQL dump** (`pg_dump`) — reproducible schema+data
- **Host on cloud storage** (S3/Drive) and add download links

## Notes & Philosophy
This repository intentionally keeps tooling minimal to emphasize SQL skills. It is not a production orchestration system — it is a learning scaffold for practicing core data engineering concepts.

---