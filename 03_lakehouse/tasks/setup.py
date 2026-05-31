#!/usr/bin/env python3
"""
Setup Studio Tasks for the retail dbt pipeline.

Compiles the dbt project, reads the compiled SQL, and creates 7 SQL tasks
in the 'retail_pipeline' folder. Task dependencies mirror the dbt model DAG:

  01_dim_customer  ─┐
  02_dim_datetime  ─┼─► 04_fct_invoices ─► 05_report_customer_invoices
  03_dim_product   ─┘                   ─► 06_report_product_invoices
                                         ─► 07_report_year_invoices

Each task runs: DROP TABLE IF EXISTS + CREATE TABLE AS SELECT
(ClickZetta does not support CREATE OR REPLACE TABLE ... AS SELECT)

The SQL comes directly from dbt compile output — so Studio Tasks always
stay in sync with the dbt models. If you change a model, re-run this script.

Prerequisites:
  - Run 03_lakehouse/setup.py first (creates cz-cli profile + dbt profiles.yml)
  - dbt-clickzetta must be installed in the current Python environment

Usage:
  cd 03_lakehouse
  python tasks/setup.py                      # reads CZ_PROFILE from .env
  python tasks/setup.py --profile retail_dev
"""
import subprocess
import json
import sys
import argparse
import os
import re
import tempfile
from pathlib import Path

try:
    from dotenv import load_dotenv
    load_dotenv(Path(__file__).parent.parent.parent / ".env")
except ImportError:
    pass

DBT_DIR = Path(__file__).parent.parent / "dbt"
FOLDER = "retail_pipeline"

# dbt model name → Studio task name + upstream deps
MODEL_DAG = [
    {"model": "dim_customer",             "task": "01_dim_customer",             "deps": []},
    {"model": "dim_datetime",             "task": "02_dim_datetime",             "deps": []},
    {"model": "dim_product",              "task": "03_dim_product",              "deps": []},
    {"model": "fct_invoices",             "task": "04_fct_invoices",             "deps": ["01_dim_customer", "02_dim_datetime", "03_dim_product"]},
    {"model": "report_customer_invoices", "task": "05_report_customer_invoices", "deps": ["04_fct_invoices"]},
    {"model": "report_product_invoices",  "task": "06_report_product_invoices",  "deps": ["04_fct_invoices"]},
    {"model": "report_year_invoices",     "task": "07_report_year_invoices",     "deps": ["04_fct_invoices"]},
]


def run_cmd(cmd, check=True, cwd=None):
    r = subprocess.run(cmd, capture_output=True, text=True, cwd=cwd)
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


def compile_dbt():
    """Run dbt compile and return path to compiled SQL directory."""
    print("  Running dbt compile...")
    rc, out = run_cmd(
        ["dbt", "compile", "--profiles-dir", "."],
        cwd=str(DBT_DIR), check=False
    )
    if rc != 0:
        print(f"  ERROR: dbt compile failed.\n  Make sure dbt-clickzetta is installed and profiles.yml exists.")
        print(f"  Run: python setup.py  (in 03_lakehouse/) first")
        sys.exit(1)
    compiled_dir = DBT_DIR / "target" / "compiled" / "retail" / "models"
    if not compiled_dir.exists():
        print(f"  ERROR: compiled directory not found: {compiled_dir}")
        sys.exit(1)
    return compiled_dir


def read_compiled_sql(compiled_dir, model_name, workspace):
    """Find compiled SQL for a model and wrap it in DROP + CREATE TABLE."""
    # search transform/ and report/ subdirs
    for subdir in ["transform", "report"]:
        sql_file = compiled_dir / subdir / f"{model_name}.sql"
        if sql_file.exists():
            select_sql = sql_file.read_text().strip()
            # The compiled SQL references the local workspace (from profiles.yml).
            # Replace it with the target workspace so the task works for any user.
            local_ws = _detect_local_workspace(select_sql)
            if local_ws and local_ws != workspace:
                select_sql = select_sql.replace(local_ws + ".", workspace + ".")
            table_ref = f"{workspace}.retail.{model_name}"
            return f"DROP TABLE IF EXISTS {table_ref};\nCREATE TABLE {table_ref} AS\n{select_sql}"
    print(f"  ERROR: compiled SQL not found for model '{model_name}'")
    sys.exit(1)


def _detect_local_workspace(sql):
    """Extract the workspace name from a compiled SQL file (e.g. 'quick_start')."""
    m = re.search(r'\b(\w+)\.retail(?:_raw)?\.', sql)
    return m.group(1) if m else None


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

    # Step 1: compile dbt to get up-to-date SQL
    print("[1/5] Compiling dbt models...")
    compiled_dir = compile_dbt()
    print(f"  compiled SQL ready at {compiled_dir}")

    # Step 2: create folder
    print(f"\n[2/5] Creating folder '{FOLDER}'...")
    run_cmd(["cz-cli", "task", "create-folder", FOLDER, "--profile", profile], check=False)

    # Step 3: create tasks (idempotent)
    print(f"\n[3/5] Creating {len(MODEL_DAG)} SQL tasks...")
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

    # Step 4: set SQL content from dbt compiled output
    print(f"\n[4/5] Setting SQL content from dbt compiled models...")
    for entry in MODEL_DAG:
        sql = read_compiled_sql(compiled_dir, entry["model"], workspace)
        with tempfile.NamedTemporaryFile(mode='w', suffix='.sql', delete=False) as f:
            f.write(sql)
            tmp = f.name
        run_cmd(["cz-cli", "task", "save-content", entry["task"],
                 "--file", tmp, "--profile", profile])
        run_cmd(["cz-cli", "task", "save-cron", entry["task"],
                 "--cron", "0 2 * * *", "--profile", profile])
        os.unlink(tmp)
        print(f"  {entry['task']} ← {entry['model']}.sql: OK")

    # Step 5: set dependencies + deploy
    print(f"\n[5/5] Setting dependencies and deploying...")
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

SQL comes from dbt compiled output — re-run this script after changing dbt models.

To execute manually:
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
