"""
generator.py - Data oversampling layer for the SDI data warehouse (Phase 1).

Creates NEW batch records for the sales (05) and hospital demand (06) source files
and writes them as separate CSV files into data/incoming/. Existing data is never
modified or removed, so the warehouse keeps its history and the new rows are appended.

Usage (from the project folder):
    python scripts/generator.py 1      # refresh 1: Jan-Mar 2026
    python scripts/generator.py 2      # refresh 2: Apr-Jun 2026

Method: bootstrap sampling of real rows, new primary keys, dates moved into the new
period, quantities scaled by a random factor (+-20% sales, +-10% hospital demand) with
all related amounts scaled together, so relationships between columns stay consistent.
Volume matches the historical monthly rate of the original files (36 months).
"""
import re
import sys
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd

SRC = Path("data/source")
OUT = Path("data/incoming")
LOG = Path("data/export/generation_log.csv")
SALES_FILE = SRC / "05_pharmacy_sales_data.csv"
DEMAND_FILE = SRC / "06_hospital_demand_data.csv"

PERIODS = {1: ("2026-01-01", "2026-03-31"), 2: ("2026-04-01", "2026-06-30"), 3: ("2026-07-01", "2026-09-30")}
BASE_MONTHS = 36  # original files cover 2023-2025


def next_ids(existing, n, offset):
    """Continue an ID sequence (same prefix and width); offset keeps refreshes from colliding."""
    existing = existing.dropna().astype(str)
    m = re.match(r"^(.*?)(\d+)$", existing.iloc[0])
    if not m:
        return [f"GEN{offset + i:09d}" for i in range(n)]
    prefix, width = m.group(1), len(m.group(2))
    start = existing.str.extract(r"(\d+)$")[0].astype("int64").max() + 1 + offset
    return [f"{prefix}{start + i:0{width}d}" for i in range(n)]


def fmt_of(sample):
    """Detect the text format of a date column from one sample value."""
    if re.match(r"^\d{4}-\d{2}$", sample):
        return "%Y-%m"
    fmt = "%d/%m/%Y" if re.match(r"^\d{1,2}/\d{1,2}/\d{4}", sample) else "%Y-%m-%d"
    if ":" in sample:
        fmt += ("T" if "T" in sample else " ") + "%H:%M:%S"
        if "." in sample:
            fmt += ".%f"
    if sample.endswith("Z"):
        fmt += "Z"
    return fmt


def parse(series):
    fmt = fmt_of(series.dropna().astype(str).iloc[0])
    return pd.to_datetime(series.astype(str), format=fmt, errors="coerce"), fmt


def shift(series, delta):
    """Move a date/timestamp column by a per-row timedelta, keeping its original text format."""
    parsed, fmt = parse(series)
    return (parsed + delta).dt.strftime(fmt).where(parsed.notna(), series)


def gen_sales(refresh, rng):
    df = pd.read_csv(SALES_FILE, low_memory=False)
    start, end = (pd.Timestamp(d) for d in PERIODS[refresh])
    months = (end.year - start.year) * 12 + end.month - start.month + 1
    n = int(round(len(df) / BASE_MONTHS * months))
    new = df.sample(n=n, replace=True, random_state=int(rng.integers(1_000_000_000))).reset_index(drop=True)
    offset = (refresh - 1) * 1_000_000

    new_day = start + pd.to_timedelta(rng.integers(0, (end - start).days + 1, n), unit="D")
    delta = pd.Series(new_day) - parse(new["SalesDate"])[0]
    for col in ["SalesDate", "SalesTimestampUTC", "PharmacyDeliveryDate", "BatchExpiryDate"]:
        if col in new.columns:
            new[col] = shift(new[col], delta)

    q_old = pd.to_numeric(new["QuantitySoldPacks"]).clip(lower=1)
    q_new = np.maximum(1, np.round(q_old * rng.uniform(0.8, 1.2, n))).astype(int)
    f = q_new / q_old
    for col in ["GrossSalesAmountUSD", "DiscountAmountUSD", "NetSalesAmountUSD",
                "SalesTaxUSD", "CostOfGoodsSoldUSD", "GrossMarginUSD"]:
        new[col] = (pd.to_numeric(new[col]) * f).round(2)
    returned = pd.to_numeric(new["ReturnedQuantityPacks"])
    new["QuantitySoldPacks"] = q_new
    new["ReturnedQuantityPacks"] = pd.Series(np.minimum(returned.values, q_new)).astype("Int64")

    new["SalesTransactionID"] = next_ids(df["SalesTransactionID"], n, offset)
    rx = new["PrescriptionID"].notna()
    if rx.any():
        new.loc[rx, "PrescriptionID"] = next_ids(df["PrescriptionID"], int(rx.sum()), offset)
    return new[df.columns], "SalesDate"


def gen_demand(refresh, rng):
    df = pd.read_csv(DEMAND_FILE, low_memory=False)
    sample = str(df["DemandMonth"].iloc[0])
    fmt = fmt_of(sample)
    period = parse(df["DemandMonth"])[0].dt.to_period("M")
    count_cols = ["EstimatedDiseaseCases", "DiseaseRelatedAdmissions", "OutpatientEncounters",
                  "ForecastDemandPacks", "RequestedQuantityPacks", "ApprovedQuantityPacks",
                  "ReceivedQuantityPacks", "EmergencyRequestQuantityPacks", "OpeningStockPacks",
                  "DispensedQuantityPacks", "ConsumedQuantityPacks", "WastageQuantityPacks",
                  "ClosingStockPacks", "UnfulfilledDemandPacks"]
    parts = []
    for p in pd.period_range(*PERIODS[refresh], freq="M"):
        base = df[period == (p - 12)].copy()  # same month one year earlier keeps seasonality
        if base.empty:
            continue
        base["DemandMonth"] = p.start_time.strftime(fmt)
        f = pd.Series(rng.uniform(0.9, 1.1, len(base)), index=base.index)  # one factor per row
        for c in count_cols:
            base[c] = np.round(pd.to_numeric(base[c]) * f).astype("Int64")
        so = pd.to_numeric(base["StockoutDays"]) * rng.uniform(0.8, 1.2, len(base))
        base["StockoutDays"] = np.clip(np.round(so), 0, p.days_in_month).astype("Int64")
        parts.append(base)
    new = pd.concat(parts, ignore_index=True)

    if refresh >= 2:  # SCD Type 2 demo: two hospitals grow, so dim_hospital gets new versions
        for h in sorted(df["HospitalID"].unique())[:2]:
            m = new["HospitalID"] == h
            new.loc[m, "LicensedBeds"] = np.round(new.loc[m, "LicensedBeds"] * 1.10).astype(int)
            new.loc[m, "ICUBeds"] = np.round(new.loc[m, "ICUBeds"] * 1.10).astype(int)
            new.loc[m, "CatchmentPopulation"] = np.round(new.loc[m, "CatchmentPopulation"] * 1.05).astype(int)

    new["HospitalDemandRecordID"] = next_ids(df["HospitalDemandRecordID"], len(new), (refresh - 1) * 100_000)
    return new[df.columns], "DemandMonth"


def main():
    if len(sys.argv) != 2 or not sys.argv[1].isdigit() or int(sys.argv[1]) not in PERIODS:
        sys.exit("Usage: python scripts/generator.py <1|2>")
    refresh = int(sys.argv[1])
    rng = np.random.default_rng(refresh * 1000 + 7)
    OUT.mkdir(parents=True, exist_ok=True)
    LOG.parent.mkdir(parents=True, exist_ok=True)

    rows = []
    for name, fn, src in [("05_pharmacy_sales_data", gen_sales, SALES_FILE),
                          ("06_hospital_demand_data", gen_demand, DEMAND_FILE)]:
        new, date_col = fn(refresh, rng)
        path = OUT / f"{name}_refresh{refresh}.csv"
        new.to_csv(path, index=False)
        dates = parse(new[date_col])[0]
        print(f"[SUCCESS] {path.name}: {len(new):,} rows, {dates.min().date()} to {dates.max().date()}")
        rows.append({"refresh": refresh, "file": path.name, "rows": len(new),
                     "period_start": PERIODS[refresh][0], "period_end": PERIODS[refresh][1],
                     "min_date": dates.min().date(), "max_date": dates.max().date(),
                     "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S")})
    pd.DataFrame(rows).to_csv(LOG, mode="a", index=False, header=not LOG.exists())


if __name__ == "__main__":
    main()

