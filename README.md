# Pharma Cold-Chain Data Warehouse (SDI) - Phase 1

**Course:** Data Analytics & Warehousing (CSE 609), Dr Tariq Mahmood
**Group:** Mehreen Faisal (36221), Areesha Zubair (36231)

Phase 1 covers layers 1-5: data generation, ingestion (batch + streaming), storage and staging (Postgres), transformation and modeling (DuckDB star schema), and the analytical query layer with optimisation. Dashboards are in Grafana.

## Repository contents
| Path | What it is |
|---|---|
| `docs/Phase.1.Report.pdf` | Phase 1 report (dashboards and Airflow design) |
| `docs/Data_Generation_Layer.pdf` | Data generation methodology and statistics |
| `docs/SDI.Excel.xlsx` | SDI workbook (requirements, OLTP, dimensions, facts, star schema) |
| `scripts/generator.py` | Oversampling layer: `python scripts\generator.py <1\|2\|3>` |
| `scripts/ingest_api.py` | Ingestion API (port 8008): `POST /ingest {"file": "..."}` |
| `scripts/file_watcher.py` | File-watcher: loads new files from `data/incoming` via the API |
| `scripts/ingest.py` | Staging loader (validation, duplicate skipping, batch log) |
| `scripts/stream_simulator.py` | Streaming ingestion simulator (throughput, latency, malformed records) |
| `scripts/build_dwh.py` | DuckDB star schema build (SCD2, junk dimension, facts) |
| `scripts/query_layer.py` | Analytical queries, indexes, summary tables, query log |
| `scripts/load_monitoring.py` | Copies DuckDB logs to Postgres schema `monitor` for Grafana |
| `dags/sdi_pipeline_full.py` | Airflow DAG: generate -> ingest + stream -> build -> queries -> monitoring |
| `dags/sdi_pipeline.py` | Original 3-task DAG |
| `dashboards/` | Grafana dashboard exports (batch, streaming, storage, query execution) |

## How to run
1. Postgres 17 with database `pharma_staging`; set `PG_PASSWORD` (the password is never stored in the repo).
2. Put the source CSVs in `data/source/` (not committed).
3. Start Airflow in WSL (`export PG_PASSWORD=...; airflow standalone`) and trigger `sdi_pipeline_full` with config `{"refresh": 2}`.
4. Or run steps manually from PowerShell:
5. Import the JSON files from `dashboards/` into Grafana (PostgreSQL data source on `pharma_staging`).

## Data volumes
Original: 500,000 sales rows and 27,216 hospital-demand rows. Each refresh adds one quarter (41,667 sales, 2,268 hospital rows). After two refreshes: 583,334 and 31,752 rows.


## Demo video
https://drive.google.com/file/d/1Q38kpiDYqMACs55ssaEwmwAFxXikdaOT/view?usp=sharing
