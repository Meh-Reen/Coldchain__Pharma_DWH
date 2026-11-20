"""
build_dwh.py - builds / refreshes the SDI star schema in DuckDB from Postgres staging (stg).

Dimensions (per SDI.Excel):  dim_date, dim_product (SCD2), dim_pharmacy, dim_customer_payer,
                             dim_warehouse, dim_sales_channel, dim_hospital (SCD2)
Facts:                       fact_sales (transactional), fact_hospital_demand (periodic)

Run (from the pharma-dwh folder, after ingest.py):
    $env:PG_PASSWORD = Read-Host "Postgres password"
    python scripts/build_dwh.py

Safe to re-run: dimensions only add new members / new versions, facts only add new rows.
"""
import os
import sys
import time
from datetime import datetime

import duckdb
import numpy as np
import pandas as pd
from sqlalchemy import create_engine

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from ingest import DB_URL  # same Postgres settings as the ingestion script

BASE = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
DWH_PATH = os.path.join(BASE, "data", "warehouse.duckdb")


# ------------------------------------------------------------------ generic helpers
def exists(con, table):
    return con.execute(
        "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = ?", [table]
    ).fetchall()[0][0] > 0


def next_key_base(con, table, key):
    if not exists(con, table):
        return 0
    return con.execute(f"SELECT GREATEST(COALESCE(MAX({key}), 0), 0) FROM {table}").fetchall()[0][0]


def create_or_insert(con, table, select_sql):
    if exists(con, table):
        con.execute(f"INSERT INTO {table} {select_sql}")
    else:
        con.execute(f"CREATE TABLE {table} AS {select_sql}")


def ensure_unknown(con, table, key):
    """Row with key -1 so facts never lose rows when a lookup fails."""
    if con.execute(f"SELECT COUNT(*) FROM {table} WHERE {key} = -1").fetchall()[0][0]:
        return
    vals = []
    for name, typ, *_ in con.execute(f"DESCRIBE {table}").fetchall():
        if name == key:
            vals.append("-1")
        elif name == "is_current":
            vals.append("TRUE")
        elif name == "effective_from":
            vals.append("DATE '1900-01-01'")
        elif name == "effective_to":
            vals.append("DATE '9999-12-31'")
        elif typ == "VARCHAR":
            vals.append("'Unknown'")
        else:
            vals.append("NULL")
    con.execute(f"INSERT INTO {table} VALUES ({', '.join(vals)})")


def norm(x):
    """Make values comparable across pandas/DuckDB (30 == 30.0, NaN == None)."""
    if x is None or (isinstance(x, float) and np.isnan(x)) or x is pd.NaT:
        return ""
    if isinstance(x, (int, float, np.integer, np.floating)) and float(x).is_integer():
        return str(int(x))
    return str(x).strip()


def merge_type1(con, table, key, nk, df):
    """No-history dimension: add members we have not seen yet."""
    df = df.drop_duplicates(subset=nk).reset_index(drop=True)
    con.register("inc", df)
    before = (con.execute(f"SELECT COUNT(*) FROM {table} WHERE {key} <> -1").fetchall()[0][0]
              if exists(con, table) else 0)
    if exists(con, table):
        cond = " AND ".join(f"d.{c} IS NOT DISTINCT FROM i.{c}" for c in nk)
        src = f"SELECT i.* FROM inc i WHERE NOT EXISTS (SELECT 1 FROM {table} d WHERE {cond})"
    else:
        src = "SELECT * FROM inc"
    base = next_key_base(con, table, key)
    order = ", ".join(nk)
    create_or_insert(con, table,
                     f"SELECT {base} + ROW_NUMBER() OVER (ORDER BY {order}) AS {key}, * FROM ({src})")
    ensure_unknown(con, table, key)
    con.unregister("inc")
    return con.execute(f"SELECT COUNT(*) FROM {table} WHERE {key} <> -1").fetchall()[0][0] - before


def merge_scd2(con, table, key, nk, tracked, inc, eff="eff"):
    """Type-2 dimension: a changed tracked column closes the old row and adds a new one."""
    inc = inc.sort_values([nk, eff]).reset_index(drop=True)
    last, last_eff = {}, {}
    if exists(con, table):
        cur = con.execute(
            f"SELECT {nk}, effective_from, {', '.join(tracked)} FROM {table} "
            f"WHERE is_current AND {key} <> -1"
        ).fetchall()
        for r in cur:
            last_eff[r[0]] = r[1]
            last[r[0]] = tuple(norm(x) for x in r[2:])
    keep = []
    for rec in inc.to_dict("records"):
        if rec[nk] in last_eff and pd.Timestamp(rec[eff]).date() <= last_eff[rec[nk]]:
            continue  # history we already hold
        sig = tuple(norm(rec[c]) for c in tracked)
        if last.get(rec[nk]) != sig:
            keep.append(rec)
            last[rec[nk]] = sig
    if not keep:
        return 0
    new = pd.DataFrame(keep)[list(inc.columns)]
    new[eff] = pd.to_datetime(new[eff]).dt.date
    con.register("newv", new)
    if exists(con, table):  # close the rows being replaced
        con.execute(f"""
            UPDATE {table}
            SET effective_to = ((SELECT MIN(n.{eff}) FROM newv n WHERE n.{nk} = {table}.{nk})
                                - INTERVAL 1 DAY)::DATE,
                is_current = FALSE
            WHERE is_current AND {key} <> -1 AND {nk} IN (SELECT {nk} FROM newv)""")
    attrs = ", ".join(c for c in new.columns if c != eff)
    base = next_key_base(con, table, key)
    create_or_insert(con, table, f"""
        SELECT {base} + ROW_NUMBER() OVER (ORDER BY {nk}, {eff}) AS {key},
               {attrs},
               {eff}::DATE AS effective_from,
               COALESCE((LEAD({eff}) OVER (PARTITION BY {nk} ORDER BY {eff}) - INTERVAL 1 DAY)::DATE,
                        DATE '9999-12-31') AS effective_to,
               LEAD({eff}) OVER (PARTITION BY {nk} ORDER BY {eff}) IS NULL AS is_current
        FROM newv""")
    ensure_unknown(con, table, key)
    con.unregister("newv")
    return len(new)


# ------------------------------------------------------------------ dimensions
def load_dim_date(con, engine):
    r = pd.read_sql("""SELECT LEAST((SELECT MIN(salesdate) FROM stg.pharmacy_sale),
                                    (SELECT MIN(demandmonth) FROM stg.hospital_demand)) AS lo,
                              GREATEST((SELECT MAX(salesdate) FROM stg.pharmacy_sale),
                                       (SELECT MAX(demandmonth) FROM stg.hospital_demand)) AS hi""",
                    engine).iloc[0]
    d = pd.DataFrame({"date": pd.date_range(f"{r.lo.year}-01-01", f"{r.hi.year + 2}-12-31")})
    out = pd.DataFrame({
        "datekey": d["date"].dt.strftime("%Y%m%d").astype(int),
        "date": d["date"].dt.date,
        "day": d["date"].dt.day,
        "monthno": d["date"].dt.month,
        "monthname": d["date"].dt.month_name(),
        "month": d["date"].dt.strftime("%Y-%m"),
        "quarter": "Q" + d["date"].dt.quarter.astype(str),
        "year": d["date"].dt.year,
        "weekday": d["date"].dt.day_name(),
    })
    con.register("inc", out)
    before = (con.execute("SELECT COUNT(*) FROM dim_date WHERE datekey <> -1").fetchall()[0][0]
              if exists(con, "dim_date") else 0)
    if exists(con, "dim_date"):
        con.execute("INSERT INTO dim_date SELECT * FROM inc WHERE datekey NOT IN (SELECT datekey FROM dim_date)")
    else:
        con.execute("CREATE TABLE dim_date AS SELECT * FROM inc")
    ensure_unknown(con, "dim_date", "datekey")
    con.unregister("inc")
    return con.execute("SELECT COUNT(*) FROM dim_date WHERE datekey <> -1").fetchall()[0][0] - before


PRODUCT_TRACKED = ["atccode", "dosageform", "strength", "routeofadministration", "packsize",
                   "prescriptionclass", "controlledmedicineflag", "coldchainflag",
                   "storagemintempc", "storagemaxtempc", "shelflifemonths", "productstatus"]


def load_dim_product(con, engine):
    df = pd.read_sql("""SELECT medicineid, internalsku, gtin14, genericname, fictionalbrandname,
            diseasecategory, icd10category, atccode, therapeuticclass, dosageform, strength,
            routeofadministration, CAST(packsize AS INTEGER) AS packsize, prescriptionclass,
            controlledmedicineflag, coldchainflag, CAST(storagemintempc AS INTEGER) AS storagemintempc,
            CAST(storagemaxtempc AS INTEGER) AS storagemaxtempc,
            CAST(shelflifemonths AS INTEGER) AS shelflifemonths, productstatus,
            effectivefromdate AS eff
        FROM stg.medicine""", engine)
    return merge_scd2(con, "dim_product", "productkey", "medicineid", PRODUCT_TRACKED, df)


HOSPITAL_TRACKED = ["licensedbeds", "icubeds", "catchmentpopulation"]


def load_dim_hospital(con, engine):
    df = pd.read_sql("""SELECT DISTINCT ON (hospitalid, demandmonth)
            hospitalid, hospitalgln, hospitalname, hospitaltype, hospitalpharmacyid, city, state,
            countrycode, CAST(licensedbeds AS INTEGER) AS licensedbeds,
            CAST(icubeds AS INTEGER) AS icubeds,
            CAST(catchmentpopulation AS INTEGER) AS catchmentpopulation, traumalevel,
            demandmonth AS eff
        FROM stg.hospital_demand ORDER BY hospitalid, demandmonth""", engine)
    return merge_scd2(con, "dim_hospital", "hospitalkey", "hospitalid", HOSPITAL_TRACKED, df)


def load_dim_pharmacy(con, engine):
    df = pd.read_sql("""SELECT DISTINCT ON (pharmacyid) pharmacyid, pharmacyname, pharmacytype,
            hospitalid, pharmacycity, pharmacystate, countrycode
        FROM stg.pharmacy_sale ORDER BY pharmacyid, salesdate DESC""", engine)
    return merge_type1(con, "dim_pharmacy", "pharmacykey", ["pharmacyid"], df)


def load_dim_customer_payer(con, engine):
    df = pd.read_sql("""SELECT DISTINCT customersegment, payertype, paymentmethod
                        FROM stg.pharmacy_sale""", engine)
    return merge_type1(con, "dim_customer_payer", "customerpayerkey",
                       ["customersegment", "payertype", "paymentmethod"], df)


def load_dim_warehouse(con, engine):
    df = pd.read_sql("SELECT DISTINCT sourcewarehouseid FROM stg.pharmacy_sale", engine)
    return merge_type1(con, "dim_warehouse", "warehousekey", ["sourcewarehouseid"], df)


def load_dim_sales_channel(con, engine):
    df = pd.read_sql("SELECT DISTINCT saleschannel FROM stg.pharmacy_sale", engine)
    return merge_type1(con, "dim_sales_channel", "channelkey", ["saleschannel"], df)


# ------------------------------------------------------------------ facts
def count(con, table):
    return con.execute(f"SELECT COUNT(*) FROM {table}").fetchall()[0][0] if exists(con, table) else 0


def load_fact_sales(con, engine):
    s = pd.read_sql("""SELECT salestransactionid, salesdate, medicineid, pharmacyid, sourcewarehouseid,
            saleschannel, customersegment, payertype, paymentmethod, pharmacyorderid,
            pharmacydeliveryid, warehouselotid, productionbatchid, quantitysoldpacks,
            deliveryquantitypacks, returnedquantitypacks, daystoexpiryatsale, grosssalesamountusd,
            discountamountusd, netsalesamountusd, salestaxusd, costofgoodssoldusd, grossmarginusd,
            deliveryotifflag FROM stg.pharmacy_sale""", engine)
    con.register("stg_sales", s)
    before = count(con, "fact_sales")
    anti = ("WHERE NOT EXISTS (SELECT 1 FROM fact_sales f WHERE f.salestransactionid = s.salestransactionid)"
            if before else "")
    base = next_key_base(con, "fact_sales", "factsaleskey")
    create_or_insert(con, "fact_sales", f"""
        SELECT {base} + ROW_NUMBER() OVER (ORDER BY s.salesdate, s.salestransactionid) AS factsaleskey,
               s.salestransactionid,
               CAST(strftime(s.salesdate, '%Y%m%d') AS INTEGER) AS datekey,
               COALESCE(p.productkey, pc.productkey, -1)        AS productkey,
               COALESCE(ph.pharmacykey, -1)                     AS pharmacykey,
               COALESCE(w.warehousekey, -1)                     AS warehousekey,
               COALESCE(ch.channelkey, -1)                      AS channelkey,
               COALESCE(cp.customerpayerkey, -1)                AS customerpayerkey,
               s.pharmacyorderid, s.pharmacydeliveryid, s.warehouselotid, s.productionbatchid,
               CAST(s.quantitysoldpacks AS INTEGER)       AS quantitysoldpacks,
               CAST(s.deliveryquantitypacks AS INTEGER)   AS deliveryquantitypacks,
               CAST(s.returnedquantitypacks AS INTEGER)   AS returnedquantitypacks,
               CAST(s.daystoexpiryatsale AS INTEGER)      AS daystoexpiryatsale,
               CAST(s.grosssalesamountusd AS DECIMAL(14,2))  AS grosssalesamountusd,
               CAST(s.discountamountusd AS DECIMAL(14,2))    AS discountamountusd,
               CAST(s.netsalesamountusd AS DECIMAL(14,2))    AS netsalesamountusd,
               CAST(s.salestaxusd AS DECIMAL(14,2))          AS salestaxusd,
               CAST(s.costofgoodssoldusd AS DECIMAL(14,2))   AS costofgoodssoldusd,
               CAST(s.grossmarginusd AS DECIMAL(14,2))       AS grossmarginusd,
               CASE WHEN s.deliveryotifflag = 'Y' THEN 1 ELSE 0 END AS deliveryotifflag
        FROM stg_sales s
        LEFT JOIN dim_product p  ON p.medicineid = s.medicineid AND p.productkey <> -1
                                AND CAST(s.salesdate AS DATE) BETWEEN p.effective_from AND p.effective_to
        LEFT JOIN dim_product pc ON pc.medicineid = s.medicineid AND pc.is_current AND pc.productkey <> -1
        LEFT JOIN dim_pharmacy ph ON ph.pharmacyid = s.pharmacyid
        LEFT JOIN dim_warehouse w ON w.sourcewarehouseid = s.sourcewarehouseid
        LEFT JOIN dim_sales_channel ch ON ch.saleschannel = s.saleschannel
        LEFT JOIN dim_customer_payer cp
               ON cp.customersegment IS NOT DISTINCT FROM s.customersegment
              AND cp.payertype       IS NOT DISTINCT FROM s.payertype
              AND cp.paymentmethod   IS NOT DISTINCT FROM s.paymentmethod
        {anti}
        ORDER BY s.salesdate, s.salestransactionid""")
    con.unregister("stg_sales")
    return count(con, "fact_sales") - before


def load_fact_hospital_demand(con, engine):
    h = pd.read_sql("""SELECT hospitaldemandrecordid, demandmonth, hospitalid, medicineid,
            forecastdemandpacks, requestedquantitypacks, approvedquantitypacks, receivedquantitypacks,
            emergencyrequestquantitypacks, unfulfilleddemandpacks, dispensedquantitypacks,
            consumedquantitypacks, wastagequantitypacks, stockoutdays, estimateddiseasecases,
            diseaserelatedadmissions, outpatientencounters, openingstockpacks, closingstockpacks,
            averageleadtimedays, servicelevelpct FROM stg.hospital_demand""", engine)
    con.register("stg_hd", h)
    before = count(con, "fact_hospital_demand")
    anti = ("WHERE NOT EXISTS (SELECT 1 FROM fact_hospital_demand f "
            "WHERE f.hospitaldemandrecordid = s.hospitaldemandrecordid)" if before else "")
    ints = ["forecastdemandpacks", "requestedquantitypacks", "approvedquantitypacks",
            "receivedquantitypacks", "emergencyrequestquantitypacks", "unfulfilleddemandpacks",
            "dispensedquantitypacks", "consumedquantitypacks", "wastagequantitypacks", "stockoutdays",
            "estimateddiseasecases", "diseaserelatedadmissions", "outpatientencounters",
            "openingstockpacks", "closingstockpacks", "averageleadtimedays"]
    int_sql = ",\n               ".join(f"CAST(s.{c} AS INTEGER) AS {c}" for c in ints)
    create_or_insert(con, "fact_hospital_demand", f"""
        SELECT s.hospitaldemandrecordid,
               CAST(strftime(s.demandmonth, '%Y%m%d') AS INTEGER) AS datekey,
               COALESCE(hs.hospitalkey, hc.hospitalkey, -1)       AS hospitalkey,
               COALESCE(p.productkey, pc.productkey, -1)          AS productkey,
               {int_sql},
               CAST(s.servicelevelpct AS DECIMAL(6,2))            AS servicelevelpct
        FROM stg_hd s
        LEFT JOIN dim_hospital hs ON hs.hospitalid = s.hospitalid AND hs.hospitalkey <> -1
                                 AND CAST(s.demandmonth AS DATE) BETWEEN hs.effective_from AND hs.effective_to
        LEFT JOIN dim_hospital hc ON hc.hospitalid = s.hospitalid AND hc.is_current AND hc.hospitalkey <> -1
        LEFT JOIN dim_product p   ON p.medicineid = s.medicineid AND p.productkey <> -1
                                 AND CAST(s.demandmonth AS DATE) BETWEEN p.effective_from AND p.effective_to
        LEFT JOIN dim_product pc  ON pc.medicineid = s.medicineid AND pc.is_current AND pc.productkey <> -1
        {anti}
        ORDER BY s.demandmonth, s.hospitaldemandrecordid""")
    con.unregister("stg_hd")
    return count(con, "fact_hospital_demand") - before


# ------------------------------------------------------------------ runner
STEPS = [
    ("dim_date", load_dim_date),
    ("dim_product", load_dim_product),
    ("dim_hospital", load_dim_hospital),
    ("dim_pharmacy", load_dim_pharmacy),
    ("dim_customer_payer", load_dim_customer_payer),
    ("dim_warehouse", load_dim_warehouse),
    ("dim_sales_channel", load_dim_sales_channel),
    ("fact_sales", load_fact_sales),
    ("fact_hospital_demand", load_fact_hospital_demand),
]


def main():
    con = duckdb.connect(DWH_PATH)
    engine = create_engine(DB_URL)
    con.execute("""CREATE TABLE IF NOT EXISTS etl_dwh_log
                   (step VARCHAR, run_ts TIMESTAMP, duration_sec DOUBLE, rows_added BIGINT,
                    status VARCHAR, error_message VARCHAR)""")
    for name, fn in STEPS:
        t0, ts = time.time(), datetime.now()
        try:
            rows, status, err = fn(con, engine), "SUCCESS", None
        except Exception as e:  # keep going so one failure is visible, not silent
            rows, status, err = 0, "FAILED", repr(e)[:500]
        dur = time.time() - t0
        con.execute("INSERT INTO etl_dwh_log VALUES (?,?,?,?,?,?)", [name, ts, dur, rows, status, err])
        print(f"[{status}] {name:<22} +{rows:,} rows  {dur:.1f}s" + (f"  {err}" if err else ""))
        if status == "FAILED":
            break  # facts depend on dimensions, so stop here
    con.close()


if __name__ == "__main__":
    main()
