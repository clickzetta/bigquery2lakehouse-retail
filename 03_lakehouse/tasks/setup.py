#!/usr/bin/env python3
"""
Setup Studio Tasks for the retail dbt pipeline.

Creates 7 SQL tasks in the 'retail_pipeline' folder, each corresponding to
a dbt model. Dependencies mirror the dbt model DAG:

  01_dim_customer  ─┐
  02_dim_datetime  ─┼─► 04_fct_invoices ─► 05_report_customer_invoices
  03_dim_product   ─┘                   ─► 06_report_product_invoices
                                         ─► 07_report_year_invoices

Each task runs: DROP TABLE IF EXISTS + CREATE TABLE AS SELECT
(ClickZetta does not support CREATE OR REPLACE TABLE ... AS SELECT)

Prerequisites:
  - Run 03_lakehouse/setup.py first (creates cz-cli profile)
  - Run dbt seed at least once to load raw data into retail_raw schema

Usage:
  python tasks/setup.py                      # reads CZ_PROFILE from .env
  python tasks/setup.py --profile retail_dev
"""
import subprocess
import json
import sys
import argparse
import os
import tempfile
import concurrent.futures
from pathlib import Path

try:
    from dotenv import load_dotenv
    load_dotenv(Path(__file__).parent.parent.parent / ".env")
except ImportError:
    pass

FOLDER = "retail_pipeline"


def get_workspace(profile):
    r = subprocess.run(
        ["cz-cli", "profile", "show", profile, "--format", "json"],
        capture_output=True, text=True
    )
    try:
        d = json.loads(r.stdout)
        return d.get("data", {}).get("workspace") or os.getenv("CLICKZETTA_WORKSPACE", "quick_start")
    except Exception:
        return os.getenv("CLICKZETTA_WORKSPACE", "quick_start")


def build_tasks(ws):
    return [
        {
            "name": "01_dim_customer",
            "sql": f"""DROP TABLE IF EXISTS {ws}.retail.dim_customer;
CREATE TABLE {ws}.retail.dim_customer AS
WITH customer_cte AS (
    SELECT DISTINCT
        md5(cast(concat(
            coalesce(cast(CustomerID as string), '_dbt_utils_surrogate_key_null_'), '-',
            coalesce(cast(Country as string), '_dbt_utils_surrogate_key_null_')
        ) as string)) AS customer_id,
        Country AS country
    FROM {ws}.retail_raw.online_retail
    WHERE CustomerID IS NOT NULL
)
SELECT t.*, cm.iso
FROM customer_cte t
LEFT JOIN {ws}.retail_raw.country cm ON t.country = cm.nicename""",
            "deps": [],
        },
        {
            "name": "02_dim_datetime",
            "sql": f"""DROP TABLE IF EXISTS {ws}.retail.dim_datetime;
CREATE TABLE {ws}.retail.dim_datetime AS
WITH datetime_cte AS (
    SELECT DISTINCT
        InvoiceDate AS datetime_id,
        TO_TIMESTAMP(
            REGEXP_REPLACE(InvoiceDate, '(\\d+)/(\\d+)/(\\d+) (\\d+):(\\d+)', '20$3-$1-$2 $4:$5'),
            'yyyy-M-d H:mm'
        ) AS ts
    FROM {ws}.retail_raw.online_retail
    WHERE InvoiceDate IS NOT NULL
)
SELECT
    datetime_id, ts AS datetime,
    DATE_FORMAT(ts, 'dd') AS day,   DATE_FORMAT(ts, 'MM') AS month,
    DATE_FORMAT(ts, 'yyyy') AS year, DATE_FORMAT(ts, 'HH') AS hour,
    DATE_FORMAT(ts, 'mm') AS minute, DAYOFWEEK(ts) AS weekday
FROM datetime_cte""",
            "deps": [],
        },
        {
            "name": "03_dim_product",
            "sql": f"""DROP TABLE IF EXISTS {ws}.retail.dim_product;
CREATE TABLE {ws}.retail.dim_product AS
SELECT DISTINCT
    md5(cast(concat(
        coalesce(cast(StockCode as string), '_dbt_utils_surrogate_key_null_'), '-',
        coalesce(cast(Description as string), '_dbt_utils_surrogate_key_null_'), '-',
        coalesce(cast(UnitPrice as string), '_dbt_utils_surrogate_key_null_')
    ) as string)) AS product_id,
    StockCode AS stock_code, Description AS description, UnitPrice AS price
FROM {ws}.retail_raw.online_retail
WHERE StockCode IS NOT NULL AND Description IS NOT NULL AND UnitPrice > 0""",
            "deps": [],
        },
        {
            "name": "04_fct_invoices",
            "sql": f"""DROP TABLE IF EXISTS {ws}.retail.fct_invoices;
CREATE TABLE {ws}.retail.fct_invoices AS
WITH fct_invoices_cte AS (
    SELECT
        InvoiceNo AS invoice_id,
        CAST(InvoiceDate AS varchar) AS datetime_id,
        md5(cast(concat(
            coalesce(cast(StockCode as string), '_dbt_utils_surrogate_key_null_'), '-',
            coalesce(cast(Description as string), '_dbt_utils_surrogate_key_null_'), '-',
            coalesce(cast(UnitPrice as string), '_dbt_utils_surrogate_key_null_')
        ) as string)) AS product_id,
        md5(cast(concat(
            coalesce(cast(CustomerID as string), '_dbt_utils_surrogate_key_null_'), '-',
            coalesce(cast(Country as string), '_dbt_utils_surrogate_key_null_')
        ) as string)) AS customer_id,
        Quantity AS quantity, Quantity * UnitPrice AS total
    FROM {ws}.retail_raw.online_retail
    WHERE Quantity > 0
)
SELECT fi.invoice_id, dt.datetime_id, dp.product_id, dc.customer_id, fi.quantity, fi.total
FROM fct_invoices_cte fi
INNER JOIN {ws}.retail.dim_datetime dt ON fi.datetime_id = dt.datetime_id
INNER JOIN {ws}.retail.dim_product dp ON fi.product_id = dp.product_id
INNER JOIN {ws}.retail.dim_customer dc ON fi.customer_id = dc.customer_id""",
            "deps": ["01_dim_customer", "02_dim_datetime", "03_dim_product"],
        },
        {
            "name": "05_report_customer_invoices",
            "sql": f"""DROP TABLE IF EXISTS {ws}.retail.report_customer_invoices;
CREATE TABLE {ws}.retail.report_customer_invoices AS
SELECT c.country, c.iso,
    COUNT(fi.invoice_id) AS total_invoices,
    SUM(fi.total) AS total_revenue
FROM {ws}.retail.fct_invoices fi
JOIN {ws}.retail.dim_customer c ON fi.customer_id = c.customer_id
GROUP BY c.country, c.iso
ORDER BY total_revenue DESC
LIMIT 10""",
            "deps": ["04_fct_invoices"],
        },
        {
            "name": "06_report_product_invoices",
            "sql": f"""DROP TABLE IF EXISTS {ws}.retail.report_product_invoices;
CREATE TABLE {ws}.retail.report_product_invoices AS
SELECT p.product_id, p.stock_code, p.description,
    SUM(fi.quantity) AS total_quantity_sold
FROM {ws}.retail.fct_invoices fi
JOIN {ws}.retail.dim_product p ON fi.product_id = p.product_id
GROUP BY p.product_id, p.stock_code, p.description
ORDER BY total_quantity_sold DESC
LIMIT 10""",
            "deps": ["04_fct_invoices"],
        },
        {
            "name": "07_report_year_invoices",
            "sql": f"""DROP TABLE IF EXISTS {ws}.retail.report_year_invoices;
CREATE TABLE {ws}.retail.report_year_invoices AS
SELECT dt.year, dt.month,
    COUNT(DISTINCT fi.invoice_id) AS num_invoices,
    SUM(fi.total) AS total_revenue
FROM {ws}.retail.fct_invoices fi
JOIN {ws}.retail.dim_datetime dt ON fi.datetime_id = dt.datetime_id
GROUP BY dt.year, dt.month
ORDER BY dt.year, dt.month""",
            "deps": ["04_fct_invoices"],
        },
    ]


def run(cmd, check=True):
    r = subprocess.run(cmd, capture_output=True, text=True)
    if check and r.returncode != 0:
        print(f"  ERROR: {r.stdout.strip()[:120]}")
        sys.exit(1)
    return r.returncode, r.stdout.strip()


def get_task_id(name, profile):
    _, out = run(["cz-cli", "task", "list", "--profile", profile, "--format", "json"], check=False)
    try:
        data = json.loads(out)
        tasks = data.get("data", [])
        for t in tasks:
            if t.get("task_name") == name:
                return t.get("task_id")
    except Exception:
        pass
    return None


def setup(profile):
    ws = get_workspace(profile)
    tasks = build_tasks(ws)

    print(f"\n=== Setup Studio Tasks (profile: {profile}, workspace: {ws}) ===\n")

    # Step 1: create folder
    print(f"[1/4] Creating folder '{FOLDER}'...")
    run(["cz-cli", "task", "create-folder", FOLDER, "--profile", profile], check=False)

    # Step 2: create tasks (idempotent)
    print(f"[2/4] Creating {len(tasks)} SQL tasks...")
    task_ids = {}
    for t in tasks:
        existing = get_task_id(t["name"], profile)
        if existing:
            task_ids[t["name"]] = existing
            print(f"  already exists: {t['name']} (id={existing})")
        else:
            run(["cz-cli", "task", "create", t["name"],
                 "--type", "SQL", "--folder", FOLDER, "--profile", profile])
            tid = get_task_id(t["name"], profile)
            task_ids[t["name"]] = tid
            print(f"  created: {t['name']} (id={tid})")

    # Step 3: set SQL content
    print(f"\n[3/4] Setting SQL content...")
    for t in tasks:
        with tempfile.NamedTemporaryFile(mode='w', suffix='.sql', delete=False) as f:
            f.write(t["sql"])
            tmp = f.name
        run(["cz-cli", "task", "save-content", t["name"],
             "--file", tmp, "--profile", profile])
        os.unlink(tmp)
        # set daily cron so deploy succeeds
        run(["cz-cli", "task", "save-cron", t["name"],
             "--cron", "0 2 * * *", "--profile", profile])
        print(f"  {t['name']}: OK")

    # Step 4: set dependencies
    print(f"\n[4/4] Setting dependencies...")
    for t in tasks:
        if not t["deps"]:
            continue
        dep_list = [{"taskId": task_ids[d], "taskName": d}
                    for d in t["deps"] if task_ids.get(d)]
        dep_json = json.dumps(dep_list)
        run(["cz-cli", "task", "save-config", t["name"],
             "--deps", "replace", "--dep-tasks", dep_json, "--profile", profile])
        print(f"  {t['name']} ← {t['deps']}")

    # Deploy all
    print(f"\nDeploying tasks...")
    for t in tasks:
        run(["cz-cli", "task", "deploy", t["name"], "--profile", profile])
        print(f"  deployed: {t['name']}")

    print(f"""
=== Setup complete ===

Task DAG (folder: {FOLDER}):
  01_dim_customer  ─┐
  02_dim_datetime  ─┼─► 04_fct_invoices ─► 05_report_customer_invoices
  03_dim_product   ─┘                   ─► 06_report_product_invoices
                                         ─► 07_report_year_invoices

To run manually:
  cz-cli task execute 01_dim_customer --profile {profile} --max-wait-seconds 60
  cz-cli task execute 02_dim_datetime --profile {profile} --max-wait-seconds 60
  cz-cli task execute 03_dim_product  --profile {profile} --max-wait-seconds 60
  cz-cli task execute 04_fct_invoices --profile {profile} --max-wait-seconds 60
  cz-cli task execute 05_report_customer_invoices --profile {profile} --max-wait-seconds 60
  cz-cli task execute 06_report_product_invoices  --profile {profile} --max-wait-seconds 60
  cz-cli task execute 07_report_year_invoices     --profile {profile} --max-wait-seconds 60
""")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--profile", default=os.getenv("CZ_PROFILE", "retail_dev"))
    args = parser.parse_args()
    setup(args.profile)
