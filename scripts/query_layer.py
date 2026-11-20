"""
query_layer.py - analytical query layer for the SDI data mart (DuckDB).

What it does
  1. creates OLAP indexes on the fact foreign keys
  2. (re)builds pre-aggregated summary tables
  3. runs the 6 SDI dimensional queries twice - baseline (on the facts) and optimized (on the
     summary tables) - and logs every execution time in table query_log
  4. exports query results + logs as CSV into data/export (for Power BI in the BI phase)

Run (from the pharma-dwh folder, after build_dwh.py):
    python scripts/query_layer.py            # timings + CSV export
    python scripts/query_layer.py show       # also prints the first rows of every result
"""
import os
import sys
import time
from datetime import datetime

import duckdb

BASE = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
DWH_PATH = os.path.join(BASE, "data", "warehouse.duckdb")
EXPORT_DIR = os.path.join(BASE, "data", "export")
RUNS = 3  # timed runs per query/variant (after 1 warm-up run)

# ------------------------------------------------------------------ indexes
INDEXES = [
    "CREATE INDEX IF NOT EXISTS idx_fs_date      ON fact_sales(datekey)",
    "CREATE INDEX IF NOT EXISTS idx_fs_product   ON fact_sales(productkey)",
    "CREATE INDEX IF NOT EXISTS idx_fs_pharmacy  ON fact_sales(pharmacykey)",
    "CREATE INDEX IF NOT EXISTS idx_fs_channel   ON fact_sales(channelkey)",
    "CREATE INDEX IF NOT EXISTS idx_fhd_date     ON fact_hospital_demand(datekey)",
    "CREATE INDEX IF NOT EXISTS idx_fhd_product  ON fact_hospital_demand(productkey)",
    "CREATE INDEX IF NOT EXISTS idx_fhd_hospital ON fact_hospital_demand(hospitalkey)",
]

# ------------------------------------------------------------------ summary tables
SUMMARIES = {
    "agg_sales_month": """
        SELECT d.year, d.quarter, d.monthno, d.month, f.channelkey, f.productkey,
               COUNT(*)                       AS txns,
               SUM(f.quantitysoldpacks)       AS quantitysoldpacks,
               SUM(f.netsalesamountusd)       AS netsalesamountusd,
               SUM(f.grossmarginusd)          AS grossmarginusd,
               SUM(f.discountamountusd)       AS discountamountusd,
               SUM(f.costofgoodssoldusd)      AS costofgoodssoldusd
        FROM fact_sales f JOIN dim_date d USING (datekey)
        GROUP BY ALL""",
    "agg_hd_month_hospital": """
        SELECT d.year, d.monthno, d.month, f.hospitalkey,
               SUM(f.forecastdemandpacks)     AS forecastdemandpacks,
               SUM(f.consumedquantitypacks)   AS consumedquantitypacks,
               SUM(f.requestedquantitypacks)  AS requestedquantitypacks,
               SUM(f.receivedquantitypacks)   AS receivedquantitypacks,
               SUM(f.unfulfilleddemandpacks)  AS unfulfilleddemandpacks,
               SUM(f.stockoutdays)            AS stockoutdays
        FROM fact_hospital_demand f JOIN dim_date d USING (datekey)
        GROUP BY ALL""",
}

# ------------------------------------------------------------------ the dimensional queries
# each: (name, business question, baseline SQL on facts, optimized SQL on summaries or None)
QUERIES = [
    ("Q1_hospital_forecast_vs_actual",
     "Hospital forecast vs actual consumption ratio, unfulfilled demand and stockout days by state and month",
     """SELECT d.year, d.monthno, h.state,
               SUM(f.forecastdemandpacks)   AS forecast_packs,
               SUM(f.consumedquantitypacks) AS consumed_packs,
               ROUND(SUM(f.consumedquantitypacks) * 1.0 / NULLIF(SUM(f.forecastdemandpacks), 0), 3) AS consumption_to_forecast,
               SUM(f.unfulfilleddemandpacks) AS unfulfilled_packs,
               SUM(f.stockoutdays)           AS stockout_days
        FROM fact_hospital_demand f
        JOIN dim_date d USING (datekey)
        JOIN dim_hospital h USING (hospitalkey)
        GROUP BY d.year, d.monthno, h.state ORDER BY d.year, d.monthno, h.state""",
     """SELECT a.year, a.monthno, h.state,
               SUM(a.forecastdemandpacks)   AS forecast_packs,
               SUM(a.consumedquantitypacks) AS consumed_packs,
               ROUND(SUM(a.consumedquantitypacks) * 1.0 / NULLIF(SUM(a.forecastdemandpacks), 0), 3) AS consumption_to_forecast,
               SUM(a.unfulfilleddemandpacks) AS unfulfilled_packs,
               SUM(a.stockoutdays)           AS stockout_days
        FROM agg_hd_month_hospital a
        JOIN dim_hospital h USING (hospitalkey)
        GROUP BY a.year, a.monthno, h.state ORDER BY a.year, a.monthno, h.state"""),

    ("Q2_channel_revenue_margin",
     "Channel revenue split and gross margin by year and quarter",
     """SELECT d.year, d.quarter, ch.saleschannel,
               COUNT(*) AS transactions,
               SUM(f.quantitysoldpacks) AS units,
               SUM(f.netsalesamountusd) AS revenue_usd,
               SUM(f.grossmarginusd)    AS margin_usd,
               ROUND(100.0 * SUM(f.grossmarginusd) / NULLIF(SUM(f.netsalesamountusd), 0), 2) AS margin_pct,
               ROUND(100.0 * SUM(f.netsalesamountusd)
                     / SUM(SUM(f.netsalesamountusd)) OVER (PARTITION BY d.year, d.quarter), 2) AS revenue_share_pct
        FROM fact_sales f
        JOIN dim_date d USING (datekey)
        JOIN dim_sales_channel ch USING (channelkey)
        GROUP BY d.year, d.quarter, ch.saleschannel ORDER BY d.year, d.quarter, ch.saleschannel""",
     """SELECT a.year, a.quarter, ch.saleschannel,
               SUM(a.txns) AS transactions,
               SUM(a.quantitysoldpacks) AS units,
               SUM(a.netsalesamountusd) AS revenue_usd,
               SUM(a.grossmarginusd)    AS margin_usd,
               ROUND(100.0 * SUM(a.grossmarginusd) / NULLIF(SUM(a.netsalesamountusd), 0), 2) AS margin_pct,
               ROUND(100.0 * SUM(a.netsalesamountusd)
                     / SUM(SUM(a.netsalesamountusd)) OVER (PARTITION BY a.year, a.quarter), 2) AS revenue_share_pct
        FROM agg_sales_month a
        JOIN dim_sales_channel ch USING (channelkey)
        GROUP BY a.year, a.quarter, ch.saleschannel ORDER BY a.year, a.quarter, ch.saleschannel"""),

    ("Q3_fast_slow_cold_chain",
     "Top 10 fast-moving and slow-moving cold-chain medicines (with therapeutic class)",
     """WITH m AS (
            SELECT p.medicineid, p.genericname, p.therapeuticclass,
                   SUM(f.quantitysoldpacks) AS units, SUM(f.netsalesamountusd) AS revenue_usd
            FROM fact_sales f JOIN dim_product p USING (productkey)
            WHERE p.coldchainflag = 'Y'
            GROUP BY p.medicineid, p.genericname, p.therapeuticclass),
        r AS (SELECT *, RANK() OVER (ORDER BY units DESC) AS fast_rank,
                         RANK() OVER (ORDER BY units ASC)  AS slow_rank FROM m)
        SELECT 'Fast-moving' AS segment, fast_rank AS rnk, genericname, therapeuticclass, units, revenue_usd
        FROM r WHERE fast_rank <= 10
        UNION ALL
        SELECT 'Slow-moving', slow_rank, genericname, therapeuticclass, units, revenue_usd
        FROM r WHERE slow_rank <= 10
        ORDER BY segment, rnk""",
     """WITH m AS (
            SELECT p.medicineid, p.genericname, p.therapeuticclass,
                   SUM(a.quantitysoldpacks) AS units, SUM(a.netsalesamountusd) AS revenue_usd
            FROM agg_sales_month a JOIN dim_product p USING (productkey)
            WHERE p.coldchainflag = 'Y'
            GROUP BY p.medicineid, p.genericname, p.therapeuticclass),
        r AS (SELECT *, RANK() OVER (ORDER BY units DESC) AS fast_rank,
                         RANK() OVER (ORDER BY units ASC)  AS slow_rank FROM m)
        SELECT 'Fast-moving' AS segment, fast_rank AS rnk, genericname, therapeuticclass, units, revenue_usd
        FROM r WHERE fast_rank <= 10
        UNION ALL
        SELECT 'Slow-moving', slow_rank, genericname, therapeuticclass, units, revenue_usd
        FROM r WHERE slow_rank <= 10
        ORDER BY segment, rnk"""),

    ("Q4_demand_vs_supply_cold_chain",
     "Cold-chain demand vs supply: requested vs received, fill rate, stockout days, unfulfilled demand by state",
     """SELECT h.state,
               SUM(f.requestedquantitypacks) AS requested_packs,
               SUM(f.receivedquantitypacks)  AS received_packs,
               SUM(f.requestedquantitypacks) - SUM(f.receivedquantitypacks) AS gap_packs,
               ROUND(100.0 * SUM(f.receivedquantitypacks) / NULLIF(SUM(f.requestedquantitypacks), 0), 2) AS fill_rate_pct,
               SUM(f.stockoutdays)           AS stockout_days,
               SUM(f.unfulfilleddemandpacks) AS unfulfilled_packs
        FROM fact_hospital_demand f
        JOIN dim_hospital h USING (hospitalkey)
        JOIN dim_product p  USING (productkey)
        WHERE p.coldchainflag = 'Y'
        GROUP BY h.state ORDER BY gap_packs DESC""",
     None),

    ("Q5_sales_vs_hospital_demand",
     "Drill-across: retail units sold vs hospital requested/received per medicine (two fact tables, one product dimension)",
     """WITH s AS (SELECT p.medicineid, SUM(f.quantitysoldpacks) AS sold_packs
                     FROM fact_sales f JOIN dim_product p USING (productkey) GROUP BY p.medicineid),
             d AS (SELECT p.medicineid, SUM(f.requestedquantitypacks) AS requested_packs,
                          SUM(f.receivedquantitypacks) AS received_packs
                     FROM fact_hospital_demand f JOIN dim_product p USING (productkey) GROUP BY p.medicineid),
             pr AS (SELECT DISTINCT medicineid, genericname, therapeuticclass
                     FROM dim_product WHERE productkey <> -1)
        SELECT pr.genericname, pr.therapeuticclass, s.sold_packs, d.requested_packs, d.received_packs,
               ROUND(100.0 * d.received_packs / NULLIF(d.requested_packs, 0), 2) AS hospital_fill_rate_pct
        FROM s JOIN d USING (medicineid) JOIN pr USING (medicineid)
        ORDER BY s.sold_packs DESC LIMIT 15""",
     """WITH s AS (SELECT p.medicineid, SUM(a.quantitysoldpacks) AS sold_packs
                     FROM agg_sales_month a JOIN dim_product p USING (productkey) GROUP BY p.medicineid),
             d AS (SELECT p.medicineid, SUM(f.requestedquantitypacks) AS requested_packs,
                          SUM(f.receivedquantitypacks) AS received_packs
                     FROM fact_hospital_demand f JOIN dim_product p USING (productkey) GROUP BY p.medicineid),
             pr AS (SELECT DISTINCT medicineid, genericname, therapeuticclass
                     FROM dim_product WHERE productkey <> -1)
        SELECT pr.genericname, pr.therapeuticclass, s.sold_packs, d.requested_packs, d.received_packs,
               ROUND(100.0 * d.received_packs / NULLIF(d.requested_packs, 0), 2) AS hospital_fill_rate_pct
        FROM s JOIN d USING (medicineid) JOIN pr USING (medicineid)
        ORDER BY s.sold_packs DESC LIMIT 15"""),

    ("Q6_monthly_revenue_trend",
     "Monthly net revenue with month-over-month growth",
     """WITH m AS (SELECT d.month, SUM(f.netsalesamountusd) AS revenue_usd
                     FROM fact_sales f JOIN dim_date d USING (datekey) GROUP BY d.month)
        SELECT month, revenue_usd,
               ROUND(100.0 * (revenue_usd / LAG(revenue_usd) OVER (ORDER BY month) - 1), 2) AS mom_growth_pct
        FROM m ORDER BY month""",
     """WITH m AS (SELECT a.month, SUM(a.netsalesamountusd) AS revenue_usd
                     FROM agg_sales_month a GROUP BY a.month)
        SELECT month, revenue_usd,
               ROUND(100.0 * (revenue_usd / LAG(revenue_usd) OVER (ORDER BY month) - 1), 2) AS mom_growth_pct
        FROM m ORDER BY month"""),
]


# ------------------------------------------------------------------ runner
def timed(con, sql):
    t0 = time.perf_counter()
    rows = con.execute(sql).fetchall()
    return (time.perf_counter() - t0) * 1000, len(rows)


def main():
    show = len(sys.argv) > 1 and sys.argv[1] == "show"
    os.makedirs(EXPORT_DIR, exist_ok=True)
    con = duckdb.connect(DWH_PATH)
    con.execute("""CREATE TABLE IF NOT EXISTS query_log
                   (query_name VARCHAR, variant VARCHAR, run_no INTEGER, run_ts TIMESTAMP,
                    exec_ms DOUBLE, rows_returned BIGINT)""")

    t0 = time.perf_counter()
    for stmt in INDEXES:
        con.execute(stmt)
    print(f"indexes ready ({(time.perf_counter() - t0) * 1000:.0f} ms)")

    for name, sql in SUMMARIES.items():
        t0 = time.perf_counter()
        con.execute(f"CREATE OR REPLACE TABLE {name} AS {sql}")
        ms = (time.perf_counter() - t0) * 1000
        n = con.execute(f"SELECT COUNT(*) FROM {name}").fetchall()[0][0]
        con.execute("INSERT INTO query_log VALUES (?,?,?,?,?,?)",
                    [name, "summary_refresh", 1, datetime.now(), ms, n])
        print(f"summary {name}: {n:,} rows ({ms:.0f} ms)")

    print(f"\n{'query':<34}{'baseline ms':>13}{'optimized ms':>14}{'speed-up':>10}")
    for name, question, base_sql, opt_sql in QUERIES:
        avg = {}
        for variant, sql in (("baseline", base_sql), ("optimized", opt_sql)):
            if sql is None:
                continue
            timed(con, sql)  # warm-up (not logged)
            times = []
            for run in range(1, RUNS + 1):
                ms, n = timed(con, sql)
                times.append(ms)
                con.execute("INSERT INTO query_log VALUES (?,?,?,?,?,?)",
                            [name, variant, run, datetime.now(), ms, n])
            avg[variant] = sum(times) / len(times)
        b, o = avg["baseline"], avg.get("optimized")
        print(f"{name:<34}{b:>13.1f}{(f'{o:.1f}' if o else '-'):>14}"
              f"{(f'{b / o:.1f}x' if o else '-'):>10}")

        final_sql = opt_sql or base_sql
        df = con.execute(final_sql).df()
        df.to_csv(os.path.join(EXPORT_DIR, f"{name}.csv"), index=False)
        if show:
            print(f"\n--- {name}: {question}\n{df.head(8).to_string(index=False)}\n")

    for table in ("query_log", "etl_dwh_log"):
        con.execute(f"COPY (SELECT * FROM {table}) TO '{os.path.join(EXPORT_DIR, table + '.csv').replace(chr(92), '/')}' (HEADER, DELIMITER ',')")
    print(f"\nCSV exports written to {EXPORT_DIR}")
    con.close()


if __name__ == "__main__":
    main()
