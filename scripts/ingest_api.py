"""
ingest_api.py - Ingestion API for the SDI staging layer (standard library only).

Start it (PowerShell, project folder):
    $env:PG_PASSWORD = "your_password"
    python scripts\ingest_api.py

Endpoints:
    GET  /health                 -> {"status": "ok"}
    GET  /status                 -> last 10 rows of stg.etl_batch_log
    POST /ingest  {"file": "05_pharmacy_sales_data_refresh1.csv"}
         loads that file from data/incoming into Postgres staging (stg) using ingest.py
         (validation, duplicate skipping, batch logging) and returns the batch-log row.
"""
import json
import os
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from sqlalchemy import create_engine, text

import ingest  # scripts/ingest.py (same folder)

BASE = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
INCOMING = os.path.join(BASE, "data", "incoming")
PORT = int(os.getenv("INGEST_API_PORT", "8008"))

engine = create_engine(ingest.DB_URL)
ingest.setup(engine)
lock = threading.Lock()  # one load at a time


def last_rows(n=10, file_name=None):
    sql = "SELECT * FROM stg.etl_batch_log"
    params = {"n": n}
    if file_name:
        sql += " WHERE file_name = :f"
        params["f"] = file_name
    sql += " ORDER BY batch_id DESC LIMIT :n"
    with engine.connect() as con:
        return [dict(r._mapping) for r in con.execute(text(sql), params)]


class Handler(BaseHTTPRequestHandler):
    def _send(self, code, obj):
        body = json.dumps(obj, default=str).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        if self.path == "/health":
            self._send(200, {"status": "ok"})
        elif self.path == "/status":
            self._send(200, last_rows(10))
        else:
            self._send(404, {"error": "not found"})

    def do_POST(self):
        if self.path != "/ingest":
            return self._send(404, {"error": "not found"})
        try:
            length = int(self.headers.get("Content-Length", 0))
            name = os.path.basename(json.loads(self.rfile.read(length))["file"])  # basename: no path tricks
        except Exception:
            return self._send(400, {"error": 'send JSON like {"file": "name.csv"}'})
        path = os.path.join(INCOMING, name)
        if not os.path.isfile(path):
            return self._send(404, {"error": f"{name} not found in data/incoming"})
        with lock:
            ingest.load_file(path, engine)
        rows = last_rows(1, name)
        self._send(200, rows[0] if rows else {"status": "UNKNOWN"})

    def log_message(self, fmt, *args):
        print("[api]", fmt % args)


if __name__ == "__main__":
    print(f"Ingestion API listening on http://localhost:{PORT}  (Ctrl+C to stop)")
    ThreadingHTTPServer(("0.0.0.0", PORT), Handler).serve_forever()
