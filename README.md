# Pharma Cold-Chain Data Warehouse (SDI)

Data warehouse and BI project for a pharmaceutical cold-chain supply chain.
Source CSVs are loaded into Postgres staging, transformed into a star schema in DuckDB,
and queried through an analytical layer. The whole flow is orchestrated by an Airflow DAG.

## Architecture

    CSV sources -> Postgres (schema stg) -> DuckDB star schema -> query layer -> CSV exports -> Power BI

| Step | Script | What it does |
|------|--------|--------------|
| 1. ingest_staging | scripts/ingest.py | Loads source CSVs into Postgres staging with validation and batch logging |
| 2. build_star_schema | scripts/build_dwh.py | Builds 7 dimensions and 2 fact tables in DuckDB, SCD Type 2 on product and hospital, unknown member (-1), logs to etl_dwh_log |
| 3. run_query_layer | scripts/query_layer.py | Builds indexes and summary tables, runs 6 analytical queries (baseline vs optimized), logs timings to query_log, exports CSVs |

The Airflow DAG is in dags/sdi_pipeline.py: ingest_staging >> build_star_schema >> run_query_layer.
The pipeline is idempotent: re-running loads 0 duplicate rows.

## Star schema

- Dimensions: dim_date, dim_product (SCD2), dim_hospital (SCD2), dim_pharmacy, dim_customer_payer (junk dimension), dim_warehouse, dim_sales_channel
- Facts: fact_sales (transactional, about 500k rows), fact_hospital_demand (periodic, about 27k rows)

## How to run

Requirements: Postgres, Python 3, DuckDB and the Python packages used in scripts/. Airflow 2.10.x runs in WSL (Ubuntu, Python 3.12) and calls the Windows Python.

Without Airflow (PowerShell, from the project folder):

    $env:PG_PASSWORD = "your_password"
    python scripts\ingest.py all
    python scripts\build_dwh.py
    python scripts\query_layer.py

With Airflow (Ubuntu):

    source ~/airflow-venv/bin/activate
    export AIRFLOW_HOME=~/airflow
    export AIRFLOW__CORE__DAGS_FOLDER=/mnt/c/Users/23amy/pharma-dwh/dags
    export AIRFLOW__CORE__LOAD_EXAMPLES=False
    export PG_PASSWORD="your_password"
    airflow standalone

Then open http://localhost:8080, enable sdi_pipeline and trigger it.
Edit WIN_PROJ and WIN_PY in the DAG to match your machine.

## Repository layout

    dags/          Airflow DAG
    scripts/       ingest, star schema build, query layer
    data/export/   query results and logs (CSV) used for Power BI dashboards
    docs/          report, SDI Excel, screenshots

## Data notes

- Only 3 cold-chain medicines exist in the data, so the top-10 fast/slow list has 3 entries.
- Q4 (demand vs supply) has no optimized version: the hospital fact is already at monthly grain.
- January 2023 revenue is very small, so month-over-month growth for the first month is not meaningful.

## Group members

- Mehreen Faisal (36221)
- Areesha Zubair (36231)

Phase 1


