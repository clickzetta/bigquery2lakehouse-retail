#!/usr/bin/env python3
"""
Setup Studio Tasks for the retail dbt pipeline.

Creates 7 SQL tasks in the 'retail_pipeline' folder. Each task runs
REFRESH DYNAMIC TABLE for one dbt model. Task dependencies mirror the dbt DAG:

  01_dim_customer  ─┐
  02_dim_datetime  ─┼─► 04_fct_invoices ─► 05_report_customer_invoices
  03_dim_product   ─┘                   ─► 06_report_product_invoices
                                         ─► 07_report_year_invoices

Why REFRESH DYNAMIC TABLE instead of dbt run:
  The original project used Airflow (schedule=None) + Cosmos to trigger dbt run
  on demand. Here, Studio Tasks replace Airflow+Cosmos as the orchestration layer.
  The dbt models are materialized as dynamic_table (no refresh_interval = no
  automatic schedule), so Studio Tasks drive when each table refreshes — exactly
  what Airflow+Cosmos did in the original project.

Prerequisites:
  - Run 03_lakehouse/setup.py first (creates cz-cli profile + dbt profiles.yml)
  - Run: dbt seed && dbt run --profiles-dir . (in 03_lakehouse/dbt/)
    to create the dynamic tables before setting up Studio Tasks

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
from pathlib import Path

try:
    from dotenv import load_dotenv
    load_dotenv(Path(__file__).parent.parent.parent / ".env")
except ImportError:
    pass

FOLDER = "retail_pipeline"

MODEL_DAG = [
    {"model": "dim_customer",             "task": "01_dim_customer",             "deps": []},
    {"model": "dim_datetime",             "task": "02_dim_datetime",             "deps": []},
    {"model": "dim_product",              "task": "03_dim_product",              "deps": []},
    {"model": "fct_invoices",             "task": "04_fct_invoices",             "deps": ["01_dim_customer", "02_dim_datetime", "03_dim_product"]},
    {"model": "report_customer_invoices", "task": "05_report_customer_invoices", "deps": ["04_fct_invoices"]},
    {"model": "report_product_invoices",  "task": "06_report_product_invoices",  "deps": ["04_fct_invoices"]},
    {"model": "report_year_invoices",     "task": "07_report_year_invoices",     "deps": ["04_fct_invoices"]},
]


def run_cmd(cmd, check=True):
    r = subprocess.run(cmd, capture_output=True, text=True)
    if check and r.returncode != 0:
        print(f"  ERROR: {r.stdout.strip()[:200] or r.stderr.strip()[:200]}")
        sys.exit(1)
    return r.returncode, r.stdout.strip()


def get_workspace(profile):
    _, out = run_cmd(["cz-cli", "profile", "show", profile, "--format", "json"], check=False)
    try:
        d = json.loads(out)
        return d.get("data", {}).get("workspace") or os.getenv("CLICKZETTA_WORKSPACE", "")
    except Exception:
        return os.getenv("CLICKZETTA_WORKSPACE", "")


def get_task_id(name, profile):
    _, out = run_cmd(["cz-cli", "task", "list", "--profile", profile, "--format", "json"], check=False)
    try:
        data = json.loads(out)
        for t in data.get("data", []):
            if t.get("task_name") == name:
                return t.get("task_id")
    except Exception:
        pass
    return None


def setup(profile):
    workspace = get_workspace(profile)
    if not workspace:
        print("ERROR: could not determine workspace. Set CLICKZETTA_WORKSPACE in .env")
        sys.exit(1)

    print(f"\n=== Setup Studio Tasks (profile: {profile}, workspace: {workspace}) ===\n")

    print(f"[1/4] Creating folder '{FOLDER}'...")
    run_cmd(["cz-cli", "task", "create-folder", FOLDER, "--profile", profile], check=False)

    print(f"\n[2/4] Creating {len(MODEL_DAG)} SQL tasks...")
    task_ids = {}
    for entry in MODEL_DAG:
        name = entry["task"]
        existing = get_task_id(name, profile)
        if existing:
            task_ids[name] = existing
            print(f"  already exists: {name} (id={existing})")
        else:
            run_cmd(["cz-cli", "task", "create", name,
                     "--type", "SQL", "--folder", FOLDER, "--profile", profile])
            tid = get_task_id(name, profile)
            task_ids[name] = tid
            print(f"  created: {name} (id={tid})")

    print(f"\n[3/4] Setting task content (REFRESH DYNAMIC TABLE)...")
    for entry in MODEL_DAG:
        table_ref = f"{workspace}.retail.{entry['model']}"
        sql = f"REFRESH DYNAMIC TABLE {table_ref};"
        with tempfile.NamedTemporaryFile(mode='w', suffix='.sql', delete=False) as f:
            f.write(sql)
            tmp = f.name
        run_cmd(["cz-cli", "task", "save-content", entry["task"],
                 "--file", tmp, "--profile", profile])
        run_cmd(["cz-cli", "task", "save-cron", entry["task"],
                 "--cron", "0 2 * * *", "--profile", profile])
        os.unlink(tmp)
        print(f"  {entry['task']}: {sql}")

    print(f"\n[4/4] Setting dependencies and deploying...")
    for entry in MODEL_DAG:
        if entry["deps"]:
            dep_list = [{"taskId": task_ids[d], "taskName": d}
                        for d in entry["deps"] if task_ids.get(d)]
            run_cmd(["cz-cli", "task", "save-config", entry["task"],
                     "--deps", "replace", "--dep-tasks", json.dumps(dep_list),
                     "--profile", profile])
        run_cmd(["cz-cli", "task", "deploy", entry["task"], "--profile", profile])
        deps_str = f" ← {entry['deps']}" if entry["deps"] else ""
        print(f"  deployed: {entry['task']}{deps_str}")

    print(f"""
=== Setup complete ===

Task DAG (folder: {FOLDER}):
  01_dim_customer  ─┐
  02_dim_datetime  ─┼─► 04_fct_invoices ─► 05_report_customer_invoices
  03_dim_product   ─┘                   ─► 06_report_product_invoices
                                         ─► 07_report_year_invoices

Each task runs: REFRESH DYNAMIC TABLE {workspace}.retail.<model>

To trigger a full pipeline refresh:
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
