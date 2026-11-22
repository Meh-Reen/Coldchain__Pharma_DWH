"""
sdi_pipeline_full - end-to-end Phase 1 pipeline.

generate_data (oversampling) -> ingest_files (file-watcher into Postgres staging)
                             -> stream_simulation (streaming ingestion, runs in parallel)
ingest_files -> build_star_schema (DuckDB) -> run_query_layer -> load_monitoring (Grafana tables)

Trigger with config {"refresh": 2} (or 3) to generate and load a new time period.
"""
import os
from datetime import datetime

from airflow import DAG
from airflow.models.param import Param
from airflow.operators.bash import BashOperator

WIN_PROJ = r"C:\Users\23amy\pharma-dwh"
WIN_PY = r"C:\Users\23amy\AppData\Local\Python\pythoncore-3.15-64\python.exe"
PW = os.environ.get("PG_PASSWORD", "")


def cmd(script_and_args):
    return ('cmd.exe /c "set PG_PASSWORD=' + PW + '&& cd /d ' + WIN_PROJ + ' && '
            + WIN_PY + ' scripts\\' + script_and_args + '"')


with DAG(
    dag_id="sdi_pipeline_full",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    params={"refresh": Param(2, type="integer", minimum=1, maximum=3)},
    tags=["sdi", "phase1"],
) as dag:
    generate_data = BashOperator(
        task_id="generate_data",
        bash_command=cmd("generator.py {{ params.refresh }}"))
    ingest_files = BashOperator(
        task_id="ingest_files",
        bash_command=cmd("file_watcher.py --once"))
    stream_simulation = BashOperator(
        task_id="stream_simulation",
        bash_command=cmd("stream_simulator.py --seconds 60 --rate 40"))
    build_star_schema = BashOperator(
        task_id="build_star_schema",
        bash_command=cmd("build_dwh.py"))
    run_query_layer = BashOperator(
        task_id="run_query_layer",
        bash_command=cmd("query_layer.py"))
    load_monitoring = BashOperator(
        task_id="load_monitoring",
        bash_command=cmd("load_monitoring.py"))

    generate_data >> [ingest_files, stream_simulation]
    ingest_files >> build_star_schema >> run_query_layer >> load_monitoring
