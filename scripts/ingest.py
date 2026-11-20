"""
ingest.py - loads source CSVs into Postgres staging (schema: stg)
with validation + batch logging.

Usage (from the pharma-dwh folder):
    python scripts/ingest.py all                         # loads the 3 SDI files from data/source
    python scripts/ingest.py data/source/05_pharmacy_sales_data.csv

Set your Postgres password first (PowerShell):
    $env:PG_PASSWORD = "your_password"
"""
import io
import os
import sys
import time
import traceback
from datetime import datetime

import pandas as pd
from sqlalchemy import create_engine, text, inspect
from sqlalchemy.engine import URL

# ------------------------------------------------------------------ config
BASE = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
SOURCE_DIR = os.path.join(BASE, "data", "source")
REJECT_DIR = os.path.join(BASE, "data", "rejected")
CHUNK = 50_000

DB_URL = URL.create(
    "postgresql+psycopg2",
    username=os.getenv("PG_USER", "postgres"),
    password=os.getenv("PG_PASSWORD", "CHANGE_ME"),
    host=os.getenv("PG_HOST", "localhost"),
    port=int(os.getenv("PG_PORT", "5432")),
    database=os.getenv("PG_DB", "pharma_staging"),
)

# Anything not listed as date/timestamp/numeric stays text.
CONFIGS = {
    "01_master_data": dict(
        table="medicine", key="MedicineID",
        required=["MedicineID", "EffectiveFromDate"],
        positive=["WholesalePriceUSD", "RecommendedRetailPriceUSD"],
        dates={"EffectiveFromDate": "%Y-%m-%d"}, timestamps=[], drop=[],
        numeric=["PackSize", "StorageMinTempC", "StorageMaxTempC", "MaxHumidityPct",
                 "ShelfLifeMonths", "MinimumRemainingShelfLifeDays",
                 "ManufacturingLeadTimeDays", "StandardBatchSizePacks", "SafetyStockDays",
                 "ReorderReviewCycleDays", "UnitManufacturingCostUSD",
                 "WholesalePriceUSD", "RecommendedRetailPriceUSD"],
    ),
    "05_pharmacy_sales_data": dict(
        table="pharmacy_sale", key="SalesTransactionID",
        required=["SalesTransactionID", "MedicineID", "SalesDate", "SalesChannel"],
        positive=["UnitRetailPriceUSD", "QuantitySoldPacks"],
        dates={"SalesDate": "%d/%m/%Y", "PharmacyDeliveryDate": "%d/%m/%Y",
               "BatchExpiryDate": "%d/%m/%Y"},
        timestamps=["SalesTimestampUTC"],
        drop=["PharmacyGLN", "GTIN14"],   # corrupted to scientific notation in the CSV
        numeric=["DeliveryQuantityPacks", "DaysToExpiryAtSale", "QuantitySoldPacks",
                 "UnitRetailPriceUSD", "GrossSalesAmountUSD", "DiscountAmountUSD",
                 "NetSalesAmountUSD", "SalesTaxUSD", "CostOfGoodsSoldUSD",
                 "GrossMarginUSD", "ReturnedQuantityPacks"],
    ),
    "06_hospital_demand_data": dict(
        table="hospital_demand", key="HospitalDemandRecordID",
        required=["HospitalDemandRecordID", "HospitalID", "MedicineID", "DemandMonth"],
        positive=[],
        dates={"DemandMonth": "%Y-%m-%d"}, timestamps=[], drop=[],
        numeric=["LicensedBeds", "ICUBeds", "AverageOccupancyRatePct", "CatchmentPopulation",
                 "EstimatedDiseaseCases", "DiseaseRelatedAdmissions", "OutpatientEncounters",
                 "ForecastDemandPacks", "RequestedQuantityPacks", "ApprovedQuantityPacks",
                 "ReceivedQuantityPacks", "EmergencyRequestQuantityPacks", "OpeningStockPacks",
                 "DispensedQuantityPacks", "ConsumedQuantityPacks", "WastageQuantityPacks",
                 "ClosingStockPacks", "UnfulfilledDemandPacks", "StockoutDays",
                 "ServiceLevelPct", "AverageLeadTimeDays", "DemandTrendIndex"],
    ),
}


# ------------------------------------------------------------------ helpers
def setup(engine):
    with engine.begin() as con:
        con.execute(text("CREATE SCHEMA IF NOT EXISTS stg"))
        con.execute(text("""
            CREATE TABLE IF NOT EXISTS stg.etl_batch_log (
                batch_id      SERIAL PRIMARY KEY,
                file_name     TEXT,
                table_name    TEXT,
                start_ts      TIMESTAMP,
                end_ts        TIMESTAMP,
                duration_sec  DOUBLE PRECISION,
                rows_read     BIGINT,
                rows_loaded   BIGINT,
                rows_rejected BIGINT,
                rows_duplicate BIGINT,
                status        TEXT,
                error_message TEXT
            )"""))


def transform(df, cfg):
    """Clean one chunk: lowercase names, drop bad columns, convert types."""
    df = df.drop(columns=[c for c in cfg["drop"] if c in df.columns])
    for c, fmt in cfg["dates"].items():
        df[c] = pd.to_datetime(df[c], format=fmt, errors="coerce")
    for c in cfg["timestamps"]:
        df[c] = pd.to_datetime(df[c], utc=True, errors="coerce")
    for c in cfg["numeric"]:
        df[c] = pd.to_numeric(df[c], errors="coerce").astype(float)
    return df


def validate(df, cfg):
    """Return a Series of reject reasons ('' = row is fine)."""
    reasons = pd.Series("", index=df.index)

    def flag(mask, msg):
        reasons[mask] = reasons[mask] + msg + "; "

    for c in cfg["required"]:
        flag(df[c].isna(), f"{c} missing/invalid")
    for c in cfg["positive"]:
        flag(~(df[c] > 0), f"{c} not > 0")
    return reasons


def copy_chunk(raw_con, table, df):
    buf = io.StringIO()
    df.to_csv(buf, index=False, header=False)
    buf.seek(0)
    cols = ",".join(df.columns)
    with raw_con.cursor() as cur:
        cur.copy_expert(f"COPY stg.{table} ({cols}) FROM STDIN WITH (FORMAT csv)", buf)


def find_config(filename, exact):
    stem = filename.rsplit(".", 1)[0]
    for prefix, cfg in CONFIGS.items():
        if (stem == prefix) if exact else stem.startswith(prefix):
            return cfg
    return None


# ------------------------------------------------------------------ main loader
def load_file(path, engine, exact=False):
    fname = os.path.basename(path)
    cfg = find_config(fname, exact)
    if cfg is None:
        print(f"[skip] no config for {fname}")
        return
    table, key = cfg["table"], cfg["key"].lower()
    start = datetime.now()
    t0 = time.time()
    rows_read = rows_loaded = rows_rejected = rows_dup = 0
    status, err = "SUCCESS", None
    raw = engine.raw_connection()

    try:
        # keys already in the table (so re-runs / overlapping files don't duplicate)
        seen = set()
        if inspect(engine).has_table(table, schema="stg"):
            seen = set(pd.read_sql(f"SELECT {key} FROM stg.{table}", engine)[key])

        os.makedirs(REJECT_DIR, exist_ok=True)
        reject_path = os.path.join(REJECT_DIR, f"{table}_rejected.csv")

        for chunk in pd.read_csv(path, dtype=str, chunksize=CHUNK):
            rows_read += len(chunk)
            chunk = transform(chunk, cfg)
            chunk.columns = [c.lower() for c in chunk.columns]
            # validation uses original-case names, so validate on a renamed cfg view
            lower_cfg = dict(cfg,
                             required=[c.lower() for c in cfg["required"]],
                             positive=[c.lower() for c in cfg["positive"]])
            reasons = validate(chunk, lower_cfg)

            bad = reasons != ""
            if bad.any():
                rej = chunk[bad].copy()
                rej["reject_reason"] = reasons[bad]
                rej.to_csv(reject_path, mode="a", index=False,
                           header=not os.path.exists(reject_path))
                rows_rejected += int(bad.sum())
            good = chunk[~bad]

            dup = good[key].isin(seen) | good[key].duplicated()
            rows_dup += int(dup.sum())
            good = good[~dup]
            seen.update(good[key])

            if good.empty:
                continue
            if not inspect(engine).has_table(table, schema="stg"):
                good.head(0).to_sql(table, engine, schema="stg", index=False)
            copy_chunk(raw, table, good)
            rows_loaded += len(good)

        raw.commit()          # whole file loads, or none of it
    except Exception:
        raw.rollback()
        status, err = "FAILED", traceback.format_exc(limit=3)
        rows_loaded = 0
        print(err)
    finally:
        raw.close()

    dur = time.time() - t0
    with engine.begin() as con:
        con.execute(text("""
            INSERT INTO stg.etl_batch_log
            (file_name, table_name, start_ts, end_ts, duration_sec, rows_read,
             rows_loaded, rows_rejected, rows_duplicate, status, error_message)
            VALUES (:f, :t, :s, :e, :d, :rr, :rl, :rj, :rd, :st, :er)"""),
            dict(f=fname, t=table, s=start, e=datetime.now(), d=dur, rr=rows_read,
                 rl=rows_loaded, rj=rows_rejected, rd=rows_dup, st=status, er=err))
    print(f"[{status}] {fname} -> stg.{table} | read={rows_read:,} loaded={rows_loaded:,} "
          f"rejected={rows_rejected:,} duplicate={rows_dup:,} | {dur:.1f}s")


if __name__ == "__main__":
    if len(sys.argv) < 2:
        sys.exit(__doc__)
    eng = create_engine(DB_URL)
    setup(eng)
    if sys.argv[1] == "all":
        for prefix in CONFIGS:      # master first, then sales, then hospital
            load_file(os.path.join(SOURCE_DIR, prefix + ".csv"), eng, exact=True)
    else:
        load_file(sys.argv[1], eng)
