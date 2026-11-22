"""
file_watcher.py - automated file-watcher for the ingestion layer.

Watches data/incoming for new CSV batch files, sends each one to the ingestion API,
then moves it to data/processed (or data/failed). If the API is not running it falls
back to loading the file directly with ingest.py, so the pipeline never stalls.

Usage (PowerShell, project folder):
    python scripts\file_watcher.py            # keep watching, check every 5 seconds
    python scripts\file_watcher.py --once     # process what is waiting, then exit
"""
import argparse
import json
import os
import shutil
import sys
import time
import urllib.error
import urllib.request

BASE = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
INCOMING = os.path.join(BASE, "data", "incoming")
PROCESSED = os.path.join(BASE, "data", "processed")
FAILED = os.path.join(BASE, "data", "failed")
API = os.getenv("INGEST_API", "http://localhost:8008")


def pending():
    os.makedirs(INCOMING, exist_ok=True)
    names = [f for f in os.listdir(INCOMING) if f.lower().endswith(".csv")]
    return sorted(names)


def wait_until_stable(path, seconds=2):
    """Skip files that are still being written."""
    while time.time() - os.path.getmtime(path) < seconds:
        time.sleep(0.5)


def call_api(name):
    req = urllib.request.Request(API + "/ingest", data=json.dumps({"file": name}).encode(),
                                 headers={"Content-Type": "application/json"}, method="POST")
    try:
        with urllib.request.urlopen(req, timeout=900) as r:
            return json.loads(r.read())
    except urllib.error.URLError:
        return None  # API not running


def load_direct(name):
    sys.path.insert(0, os.path.join(BASE, "scripts"))
    import ingest
    from sqlalchemy import create_engine, text
    eng = create_engine(ingest.DB_URL)
    ingest.setup(eng)
    ingest.load_file(os.path.join(INCOMING, name), eng)
    with eng.connect() as con:
        row = con.execute(text("SELECT * FROM stg.etl_batch_log WHERE file_name=:f "
                               "ORDER BY batch_id DESC LIMIT 1"), {"f": name}).fetchone()
    return dict(row._mapping) if row else {"status": "UNKNOWN"}


def move(name, folder):
    os.makedirs(folder, exist_ok=True)
    target = os.path.join(folder, name)
    if os.path.exists(target):
        target = os.path.join(folder, time.strftime("%Y%m%d_%H%M%S_") + name)
    shutil.move(os.path.join(INCOMING, name), target)


def process(name):
    wait_until_stable(os.path.join(INCOMING, name))
    print(f"[watcher] new file: {name}")
    res = call_api(name)
    mode = "api"
    if res is None:
        mode = "direct (API not running)"
        res = load_direct(name)
    ok = res.get("status") == "SUCCESS"
    move(name, PROCESSED if ok else FAILED)
    print(f"[watcher] {name}: {res.get('status')} via {mode} | loaded={res.get('rows_loaded')} "
          f"rejected={res.get('rows_rejected')} duplicate={res.get('rows_duplicate')} -> "
          f"{'processed' if ok else 'failed'}")
    return ok


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--once", action="store_true")
    ap.add_argument("--interval", type=int, default=5)
    args = ap.parse_args()
    if args.once:
        files = pending()
        print(f"[watcher] {len(files)} file(s) waiting in data/incoming")
        results = [process(f) for f in files]
        sys.exit(0 if all(results) else 1)
    print(f"[watcher] watching {INCOMING} every {args.interval}s (Ctrl+C to stop)")
    while True:
        for f in pending():
            process(f)
        time.sleep(args.interval)
