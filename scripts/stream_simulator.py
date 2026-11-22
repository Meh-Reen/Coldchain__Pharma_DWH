r"""
stream_simulator.py - streaming ingestion simulator for the SDI staging layer.

Sends a continuous stream of sales events into Postgres (stg.stream_sales), one batch per
second. Each event is validated; malformed events (null medicine key, quantity <= 0,
missing or non-positive price) are rejected and counted. A metrics row is written every
second to stg.stream_metrics for the Grafana streaming dashboard:
    throughput (events/sec), malformed count, end-to-end latency (insert time - event time).
About 5% of events arrive late (5-30 s after they happened) to mimic real streams.

Usage (PowerShell, project folder):
    $env:PG_PASSWORD = "your_password"
    python scripts\stream_simulator.py --seconds 120 --rate 40
"""
import argparse
import random
import statistics
import time
from datetime import datetime, timedelta, timezone

from sqlalchemy import create_engine, text

import ingest  # scripts/ingest.py (database settings)

SALES = "stg.stream_sales"
METRICS = "stg.stream_metrics"

DDL = [
    f"""CREATE TABLE IF NOT EXISTS {SALES} (
        event_id TEXT PRIMARY KEY, run_id TEXT, event_ts TIMESTAMPTZ, ingest_ts TIMESTAMPTZ,
        latency_sec DOUBLE PRECISION, medicine_id TEXT, pharmacy_id TEXT, sales_channel TEXT,
        quantity INTEGER, unit_price DOUBLE PRECISION)""",
    f"""CREATE TABLE IF NOT EXISTS {METRICS} (
        run_id TEXT, ts TIMESTAMPTZ, events_received INTEGER, events_ingested INTEGER,
        malformed INTEGER, throughput_eps DOUBLE PRECISION,
        avg_latency_sec DOUBLE PRECISION, max_latency_sec DOUBLE PRECISION)""",
]
INSERT_SALES = text(f"""INSERT INTO {SALES}
    (event_id, run_id, event_ts, ingest_ts, latency_sec, medicine_id, pharmacy_id,
     sales_channel, quantity, unit_price)
    VALUES (:event_id, :run_id, :event_ts, :ingest_ts, :latency_sec, :medicine_id,
            :pharmacy_id, :sales_channel, :quantity, :unit_price)""")
INSERT_METRIC = text(f"""INSERT INTO {METRICS}
    (run_id, ts, events_received, events_ingested, malformed, throughput_eps,
     avg_latency_sec, max_latency_sec)
    VALUES (:run_id, :ts, :received, :ingested, :malformed, :eps, :avg_lat, :max_lat)""")


def find_col(cols, *words):
    """Find a staging column whose lower-case name contains all the given words."""
    for c in cols:
        if all(w in c.lower() for w in words):
            return c
    raise SystemExit(f"column with {words} not found in stg.pharmacy_sale: {cols}")


def load_reference(con):
    """Real medicine / pharmacy / channel values and typical prices from the staging data."""
    cols = [r[0] for r in con.execute(text(
        "SELECT column_name FROM information_schema.columns "
        "WHERE table_schema='stg' AND table_name='pharmacy_sale'"))]
    med = find_col(cols, "medicine", "id")
    ph = find_col(cols, "pharmacy", "id")
    ch = find_col(cols, "channel")
    pr = find_col(cols, "unit", "price")
    q = lambda c: '"' + c + '"'
    meds = [r[0] for r in con.execute(text(f"SELECT DISTINCT {q(med)} FROM stg.pharmacy_sale"))]
    pharm = [r[0] for r in con.execute(text(f"SELECT DISTINCT {q(ph)} FROM stg.pharmacy_sale"))]
    chan = [r[0] for r in con.execute(text(f"SELECT DISTINCT {q(ch)} FROM stg.pharmacy_sale"))]
    price = {r[0]: float(r[1]) for r in con.execute(text(
        f"SELECT {q(med)}, AVG({q(pr)}::float) FROM stg.pharmacy_sale GROUP BY {q(med)}"))}
    return {"meds": meds, "pharmacies": pharm, "channels": chan, "price": price}


def make_event(ref, run_id, seq, now):
    med = random.choice(ref["meds"])
    late = random.random() < 0.05
    lag = random.uniform(5, 30) if late else random.uniform(0.1, 2.0)
    return {"event_id": f"{run_id}-{seq:08d}", "run_id": run_id,
            "event_ts": now - timedelta(seconds=lag), "medicine_id": med,
            "pharmacy_id": random.choice(ref["pharmacies"]),
            "sales_channel": random.choice(ref["channels"]),
            "quantity": random.randint(1, 12),
            "unit_price": round(ref["price"].get(med, 20.0) * random.uniform(0.95, 1.05), 2)}


def corrupt(ev):
    kind = random.choice(["null_key", "bad_qty", "bad_price"])
    if kind == "null_key":
        ev["medicine_id"] = None
    elif kind == "bad_qty":
        ev["quantity"] = random.choice([0, -1])
    else:
        ev["unit_price"] = random.choice([None, -5.0])
    return ev


def validate(ev):
    if not ev["medicine_id"]:
        raise ValueError("null medicine_id")
    if ev["quantity"] is None or ev["quantity"] <= 0:
        raise ValueError("quantity not > 0")
    if ev["unit_price"] is None or ev["unit_price"] <= 0:
        raise ValueError("unit_price missing or not > 0")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--seconds", type=int, default=120)
    ap.add_argument("--rate", type=int, default=40, help="average events per second")
    ap.add_argument("--bad", type=float, default=0.03, help="share of malformed events")
    args = ap.parse_args()

    engine = create_engine(ingest.DB_URL)
    with engine.begin() as con:
        for ddl in DDL:
            con.execute(text(ddl))
        ref = load_reference(con)
    run_id = datetime.now().strftime("%Y%m%d_%H%M%S")
    print(f"[stream] run {run_id}: {args.seconds}s at about {args.rate} events/s "
          f"(Ctrl+C to stop early)")

    seq = total_in = total_bad = 0
    lats = []
    t_start = time.time()
    try:
        for tick in range(args.seconds):
            t0 = time.time()
            now = datetime.now(timezone.utc)
            n = max(1, int(random.gauss(args.rate, args.rate * 0.15)))
            good, bad = [], 0
            for _ in range(n):
                seq += 1
                ev = make_event(ref, run_id, seq, now)
                if random.random() < args.bad:
                    ev = corrupt(ev)
                try:
                    validate(ev)
                    good.append(ev)
                except ValueError:
                    bad += 1
            time.sleep(max(0.0, 1.0 - (time.time() - t0)))   # one batch per second
            ingest_ts = datetime.now(timezone.utc)
            for ev in good:
                ev["ingest_ts"] = ingest_ts
                ev["latency_sec"] = (ingest_ts - ev["event_ts"]).total_seconds()
            lat = [ev["latency_sec"] for ev in good]
            window = max(time.time() - t0, 0.001)
            with engine.begin() as con:
                if good:
                    con.execute(INSERT_SALES, good)
                con.execute(INSERT_METRIC, dict(
                    run_id=run_id, ts=datetime.now(timezone.utc), received=n, ingested=len(good),
                    malformed=bad, eps=len(good) / window,
                    avg_lat=statistics.mean(lat) if lat else 0.0, max_lat=max(lat) if lat else 0.0))
            total_in += len(good)
            total_bad += bad
            lats.extend(lat)
            if (tick + 1) % 10 == 0:
                print(f"[stream] {tick + 1}s  ingested={total_in:,}  malformed={total_bad:,}")
    except KeyboardInterrupt:
        print("[stream] stopped early")
    dur = max(time.time() - t_start, 0.001)
    print(f"[SUCCESS] stream run {run_id}: ingested={total_in:,} malformed={total_bad:,} "
          f"avg throughput={total_in / dur:.1f} events/s "
          f"avg latency={statistics.mean(lats) if lats else 0:.2f}s")


if __name__ == "__main__":
    main()
