"""load_monitoring.py - copies DuckDB log tables into Postgres schema 'monitor' for Grafana."""
import os
import duckdb
from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL

url = URL.create(
    "postgresql+psycopg2",
    username=os.getenv("PG_USER", "postgres"),
    password=os.environ["PG_PASSWORD"],
    host=os.getenv("PG_HOST", "localhost"),
    port=int(os.getenv("PG_PORT", "5432")),
    database=os.getenv("PG_DB", "pharma_staging"),
)
engine = create_engine(url)
con = duckdb.connect("data/warehouse.duckdb", read_only=True)

with engine.begin() as c:
    c.execute(text("CREATE SCHEMA IF NOT EXISTS monitor"))

for t in ["etl_dwh_log", "query_log"]:
    df = con.execute(f"SELECT * FROM {t}").df()
    df.to_sql(t, engine, schema="monitor", if_exists="replace", index=False)
    print(f"[SUCCESS] monitor.{t}: {len(df):,} rows")
