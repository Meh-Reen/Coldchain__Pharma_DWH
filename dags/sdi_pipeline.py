"""
sdi_pipeline.py - Airflow DAG for the Pharma Cold-Chain SDI data warehouse.

Airflow runs inside WSL (Ubuntu). Each task calls the project's Windows Python
(where Postgres + DuckDB live) through cmd.exe, so the scripts run unchanged.

    ingest_staging  ->  build_star_schema  ->  run_query_layer

Setup: export PG_PASSWORD in the Ubuntu shell BEFORE starting Airflow.
Adjust WIN_PROJ and WIN_PY to your machine.
"""
import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

WIN_PROJ = r"C:\Users\23amy\pharma-dwh"
WIN_PY = r"C:\Users\23amy\AppData\Local\Python\pythoncore-3.15-64\python.exe"
PG_PASSWORD = os.environ.get("PG_PASSWORD", "")

# No space before && or it becomes part of the password.
CMD = 'cmd.exe /c "set PG_PASSWORD={pw}&& cd /d {proj} && {py} scripts\\{script}"'


def cmd(script):
    return CMD.format(pw=PG_PASSWORD, proj=WIN_PROJ, py=WIN_PY, script=script)


default_args = {
    "owner": "sdi_team",
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

with DAG(
    dag_id="sdi_pipeline",
    description="Ingest -> star schema -> analytical queries for the SDI data mart",
    default_args=default_args,
    start_date=datetime(2026, 11, 1),
    schedule=None,
    catchup=False,
    tags=["sdi", "dwh", "pharma"],
) as dag:

    ingest = BashOperator(task_id="ingest_staging", bash_command=cmd("ingest.py all"))
    build = BashOperator(task_id="build_star_schema", bash_command=cmd("build_dwh.py"))
    queries = BashOperator(task_id="run_query_layer", bash_command=cmd("query_layer.py"))

    ingest >> build >> queries
