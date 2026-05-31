#!/usr/bin/env python3
"""
Setup Studio Tasks for the retail dbt pipeline.

Creates 7 SQL tasks in the 'retail_pipeline' folder. Each task runs
REFRESH DYNAMIC TABLE for one dbt model. Task dependencies mirror the dbt DAG:

  01_dim_customer  ─┐
  02_dim_datetime  ─┼─► 04_fct_invoices ─► 05_report_customer_invoices
  03_dim_product   ─┘                   ─► 06_report_product_invoices
                                         ─► 07_report_year_invoices

Also generates two local SQL directories (gitignored):
  tasks/ddl/     — CREATE DYNAMIC TABLE DDL (compiled from dbt, for reference)
  tasks/refresh/ — REFRESH DYNAMIC TABLE commands (used as Studio Task content)

Why REFRESH DYNAMIC TABLE instead of dbt run:
  The original project used Airflow (schedule=None) + Cosmos to trigger dbt run
  on demand. Here, Studio Tasks replace Airflow+Cosmos as the orchestration layer.
  The dbt models are materialized as dynamic_table (no refresh_interval = no
  automatic schedule), so Studio Tasks drive when each table refreshes — exactly
  what Airflow+Cosmos did in the original project.

Prerequisites:
  - Run 03_lakehouse/setup.py first (creates cz-cli profile + dbt profiles.yml)
  - Run: dbt seed && dbt run --profiles-dir . (in 03_lakehouse/dbt/)

Usage:
  python tasks/setup.py                      # reads CZ_PROFILE from .env
  python tasks/setup.py --profile retail_dev
"""
import subprocess
import json
import sys
import argparse
import os
import re
from pathlib import Path

try:
    from dotenv import load_dotenv
    load_dotenv(Path(__file__).parent.parent.parent / ".env")
except ImportError:
    pass

TASKS_DIR = Path(__file__).parent
DBT_DIR = TASKS_DIR.parent / "dbt"
FOLDER = "retail_pipeline"
FOLDER_INIT = "retail_pipeline_init"

MODEL_DAG = [
    {"model": "dim_customer",             "task": "01_dim_customer",             "subdir": "transform", "deps": []},
    {"model": "dim_datetime",             "task": "02_dim_datetime",             "subdir": "transform", "deps": []},
    {"model": "dim_product",              "task": "03_dim_product",              "subdir": "transform", "deps": []},
    {"model": "fct_invoices",             "task": "04_fct_invoices",             "subdir": "transform", "deps": ["01_dim_customer", "02_dim_datetime", "03_dim_product"]},
    {"model": "report_customer_invoices", "task": "05_report_customer_invoices", "subdir": "report",    "deps": ["04_fct_invoices"]},
    {"model": "report_product_invoices",  "task": "06_report_product_invoices",  "subdir": "report",    "deps": ["04_fct_invoices"]},
    {"model": "report_year_invoices",     "task": "07_report_year_invoices",     "subdir": "report",    "deps": ["04_fct_invoices"]},
]


def run_cmd(cmd, check=True):
    r = subprocess.run(cmd, capture_output=True, text=True)
    if check and r.returncode != 0:
        print(f"  ERROR: {r.stdout.strip()[:200] or r.stderr.strip()[:200]}")
        sys.exit(1)
    return r.returncode, r.stdout.strip()


def compile_dbt():
    print("  Running dbt compile...")
    r = subprocess.run(["dbt", "compile", "--profiles-dir", "."],
                       capture_output=True, text=True, cwd=str(DBT_DIR))
    if r.returncode != 0:
        print("  ERROR: dbt compile failed. Run setup.py first to create profiles.yml.")
        sys.exit(1)
    run_dir = DBT_DIR / "target" / "run" / "retail" / "models"
    if not run_dir.exists():
        print(f"  ERROR: run directory not found: {run_dir}")
        print("  Run: dbt run --profiles-dir . first")
        sys.exit(1)
    return run_dir


def generate_sql_files(workspace, run_dir):
    ddl_dir = TASKS_DIR / "ddl"
    refresh_dir = TASKS_DIR / "refresh"
    ddl_dir.mkdir(exist_ok=True)
    refresh_dir.mkdir(exist_ok=True)

    # detect local workspace embedded in run SQL
    sample = (run_dir / "transform" / "dim_customer.sql").read_text()
    m = re.search(r'create dynamic table\s+(\w+)\.retail', sample, re.IGNORECASE)
    local_ws = m.group(1) if m else workspace

    for entry in MODEL_DAG:
        sql_file = run_dir / entry["subdir"] / f"{entry['model']}.sql"
        # dbt run SQL is the exact DDL executed — replace local workspace with target
        ddl_sql = sql_file.read_text().strip().replace(local_ws + ".", workspace + ".")
        table_ref = f"{workspace}.retail.{entry['model']}"

        (ddl_dir / f"{entry['task']}.sql").write_text(ddl_sql)
        (refresh_dir / f"{entry['task']}.sql").write_text(
            f"REFRESH DYNAMIC TABLE {table_ref};"
        )

    print(f"  ddl/     — {len(MODEL_DAG)} CREATE DYNAMIC TABLE files (from dbt target/run/)")
    print(f"  refresh/ — {len(MODEL_DAG)} REFRESH DYNAMIC TABLE files")


def get_workspace(profile):
    _, out = run_cmd(["cz-cli", "profile", "show", profile, "--format", "json"], check=False)
    try:
        d = json.loads(out)
        return d.get("data", {}).get("workspace") or os.getenv("CLICKZETTA_WORKSPACE", "")
    except Exception:
        return os.getenv("CLICKZETTA_WORKSPACE", "")


def get_task_id(name, profile):
    _, out = run_cmd(["cz-cli", "task", "list", "--profile", profile,
                      "--format", "json", "--page-size", "100"], check=False)
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

    # Step 1: read dbt run output + generate SQL files
    print("[1/5] Reading dbt target/run/ and generating SQL files...")
    run_dir = compile_dbt()
    generate_sql_files(workspace, run_dir)

    # Step 2: create folders
    print(f"\n[2/6] Creating folders '{FOLDER}' and '{FOLDER_INIT}'...")
    run_cmd(["cz-cli", "task", "create-folder", FOLDER, "--profile", profile], check=False)
    run_cmd(["cz-cli", "task", "create-folder", FOLDER_INIT, "--profile", profile], check=False)

    # Step 3: create refresh tasks (idempotent)
    print(f"\n[3/6] Creating {len(MODEL_DAG)} refresh tasks in '{FOLDER}'...")
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

    # Step 4: create init (DDL) tasks (idempotent)
    init_name = lambda t: f"init_{t}"
    print(f"\n[4/6] Creating {len(MODEL_DAG)} init tasks in '{FOLDER_INIT}'...")
    for entry in MODEL_DAG:
        name = init_name(entry["task"])
        existing = get_task_id(name, profile)
        if existing:
            print(f"  already exists: {name} (id={existing})")
        else:
            run_cmd(["cz-cli", "task", "create", name,
                     "--type", "SQL", "--folder", FOLDER_INIT, "--profile", profile])
            print(f"  created: {name}")

    # Step 5: set content + cron + deploy
    print(f"\n[5/6] Setting task content and deploying...")
    for entry in MODEL_DAG:
        # refresh task
        refresh_file = TASKS_DIR / "refresh" / f"{entry['task']}.sql"
        run_cmd(["cz-cli", "task", "save-content", entry["task"],
                 "--file", str(refresh_file), "--profile", profile])
        run_cmd(["cz-cli", "task", "save-cron", entry["task"],
                 "--cron", "0 2 * * *", "--profile", profile])
        # init task (DDL, no cron — run manually to create or rebuild tables)
        ddl_file = TASKS_DIR / "ddl" / f"{entry['task']}.sql"
        run_cmd(["cz-cli", "task", "save-content", init_name(entry["task"]),
                 "--file", str(ddl_file), "--profile", profile])
        print(f"  {entry['task']}: refresh + init content set")

    # Step 6: set dependencies + deploy refresh tasks; init tasks stay in draft
    print(f"\n[6/6] Setting dependencies and deploying refresh tasks...")
    for entry in MODEL_DAG:
        if entry["deps"]:
            dep_list = [{"taskId": task_ids[d], "taskName": d}
                        for d in entry["deps"] if task_ids.get(d)]
            run_cmd(["cz-cli", "task", "save-config", entry["task"],
                     "--deps", "replace", "--dep-tasks", json.dumps(dep_list),
                     "--profile", profile])
        run_cmd(["cz-cli", "task", "deploy", entry["task"], "--profile", profile])
        # init tasks stay in draft — no deploy, no schedule, run manually
        deps_str = f" ← {entry['deps']}" if entry["deps"] else ""
        print(f"  deployed: {entry['task']}{deps_str}  |  draft: {init_name(entry['task'])}")

    print(f"""
=== Setup complete ===

Generated files (gitignored, workspace-specific):
  tasks/ddl/     — CREATE DYNAMIC TABLE DDL (from dbt target/run/, used by init tasks)
  tasks/refresh/ — REFRESH DYNAMIC TABLE commands (used by refresh tasks)

Studio folders:
  {FOLDER}/       — 7 refresh tasks (daily schedule, dependency chain, deployed)
  {FOLDER_INIT}/  — 7 init tasks (CREATE DYNAMIC TABLE, draft, run manually to rebuild)

Task DAG ({FOLDER}):
  01_dim_customer  ─┐
  02_dim_datetime  ─┼─► 04_fct_invoices ─► 05_report_customer_invoices
  03_dim_product   ─┘                   ─► 06_report_product_invoices
                                         ─► 07_report_year_invoices

To trigger a full pipeline refresh:
  cz-cli task execute 01_dim_customer --profile {profile} --max-wait-seconds 60
  cz-cli task execute 02_dim_datetime --profile {profile} --max-wait-seconds 60
  cz-cli task execute 03_dim_product  --profile {profile} --max-wait-seconds 60
  cz-cli task execute 04_fct_invoices --profile {profile} --max-wait-seconds 60
  cz-cli task execute 05_report_customer_invoices --profile {profile} --max-wait-seconds 60
  cz-cli task execute 06_report_product_invoices  --profile {profile} --max-wait-seconds 60
  cz-cli task execute 07_report_year_invoices     --profile {profile} --max-wait-seconds 60

To rebuild a table (e.g. after schema change):
  cz-cli task execute init_01_dim_customer --profile {profile} --max-wait-seconds 60
""")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--profile", default=os.getenv("CZ_PROFILE", "retail_dev"))
    args = parser.parse_args()
    setup(args.profile)
