#!/usr/bin/env python3
"""
Teardown script for bigquery2lakehouse-retail.

Removes all Lakehouse objects and Studio Tasks created by this project:
  - Dynamic tables in retail schema
  - Seed tables in retail_raw schema
  - Studio Tasks in retail_pipeline/ and retail_pipeline_init/ folders

Usage:
  python tasks/teardown.py                      # reads CZ_PROFILE from .env
  python tasks/teardown.py --profile retail_dev
  python tasks/teardown.py --profile retail_dev --yes   # skip confirmation
"""
import subprocess
import json
import sys
import argparse
import os
from pathlib import Path

try:
    from dotenv import load_dotenv
    load_dotenv(Path(__file__).parent.parent.parent / ".env")
except ImportError:
    pass

DYNAMIC_TABLES = [
    "dim_customer", "dim_datetime", "dim_product", "fct_invoices",
    "report_customer_invoices", "report_product_invoices", "report_year_invoices",
]
SEED_TABLES = ["online_retail", "country"]
REFRESH_TASKS = [
    "01_dim_customer", "02_dim_datetime", "03_dim_product", "04_fct_invoices",
    "05_report_customer_invoices", "06_report_product_invoices", "07_report_year_invoices",
]
INIT_TASKS = [f"init_{t}" for t in REFRESH_TASKS]
FOLDERS = ["retail_pipeline", "retail_pipeline_init"]
FOLDER_PARENT = "bigquery2lakehouse_retail"
FOLDER_IDS_FILE = Path(__file__).parent / ".folder_ids.json"


def run_cmd(cmd, check=False):
    r = subprocess.run(cmd, capture_output=True, text=True)
    return r.returncode, r.stdout.strip()


def get_workspace(profile):
    _, out = run_cmd(["cz-cli", "profile", "show", profile, "--format", "json"])
    try:
        d = json.loads(out)
        return d.get("data", {}).get("workspace") or os.getenv("CLICKZETTA_WORKSPACE", "")
    except Exception:
        return os.getenv("CLICKZETTA_WORKSPACE", "")


def teardown(profile, yes=False):
    workspace = get_workspace(profile)
    if not workspace:
        print("ERROR: could not determine workspace. Set CLICKZETTA_WORKSPACE in .env")
        sys.exit(1)

    print(f"\n=== Teardown bigquery2lakehouse-retail (profile: {profile}, workspace: {workspace}) ===\n")
    print("This will remove:")
    print(f"  Dynamic tables : {workspace}.retail.{{dim_customer, dim_datetime, ...}} ({len(DYNAMIC_TABLES)} tables)")
    print(f"  Seed tables    : {workspace}.retail_raw.{{online_retail, country}}")
    print(f"  Studio Tasks   : {len(REFRESH_TASKS + INIT_TASKS)} tasks in retail_pipeline/ and retail_pipeline_init/")
    print()

    if not yes:
        answer = input("Proceed? [y/N] ").strip().lower()
        if answer != "y":
            print("Cancelled.")
            sys.exit(0)

    # Drop dynamic tables
    print("[1/3] Dropping dynamic tables...")
    for t in DYNAMIC_TABLES:
        rc, out = run_cmd(["cz-cli", "sql", f"DROP DYNAMIC TABLE IF EXISTS {workspace}.retail.{t}",
                           "--profile", profile, "--sync", "--write"])
        print(f"  {'OK' if rc==0 and 'error' not in out else 'SKIP'} {workspace}.retail.{t}")

    # Drop seed tables
    print("\n[2/3] Dropping seed tables...")
    for t in SEED_TABLES:
        rc, out = run_cmd(["cz-cli", "sql", f"DROP TABLE IF EXISTS {workspace}.retail_raw.{t}",
                           "--profile", profile, "--sync", "--write"])
        print(f"  {'OK' if rc==0 and 'error' not in out else 'SKIP'} {workspace}.retail_raw.{t}")

    # Delete Studio Tasks
    print("\n[3/3] Deleting Studio Tasks...")
    for task in REFRESH_TASKS + INIT_TASKS:
        run_cmd(["cz-cli", "task", "undeploy", task, "--profile", profile])
        rc, out = run_cmd(["cz-cli", "task", "delete", task, "--yes", "--profile", profile])
        print(f"  {'OK' if rc==0 else 'SKIP'} {task}")

    # Delete folders using saved IDs (sub-folders not returned by list-folders API)
    print("\nDeleting folders...")
    saved_ids = {}
    if FOLDER_IDS_FILE.exists():
        saved_ids = json.loads(FOLDER_IDS_FILE.read_text())

    # delete sub-folders by ID first
    for name in FOLDERS:
        fid = saved_ids.get(name)
        if fid:
            rc, out = run_cmd(["cz-cli", "task", "delete-folder", str(fid),
                               "--yes", "--profile", profile])
            print(f"  {'OK' if rc==0 else 'SKIP'} sub-folder: {name} (id={fid})")
        else:
            print(f"  SKIP sub-folder: {name} (no saved ID)")

    # delete parent folder by ID or name
    parent_id = saved_ids.get(FOLDER_PARENT)
    if parent_id:
        rc, out = run_cmd(["cz-cli", "task", "delete-folder", str(parent_id),
                           "--yes", "--profile", profile])
        print(f"  {'OK' if rc==0 else 'SKIP'} parent folder: {FOLDER_PARENT} (id={parent_id})")
    else:
        rc, out = run_cmd(["cz-cli", "task", "delete-folder", FOLDER_PARENT,
                           "--yes", "--profile", profile])
        print(f"  {'OK' if rc==0 else 'SKIP'} parent folder: {FOLDER_PARENT}")

    # clean up saved IDs file
    if FOLDER_IDS_FILE.exists():
        FOLDER_IDS_FILE.unlink()
        print("  cleaned up .folder_ids.json")

    print("\n=== Teardown complete ===")
    print("Note: retail and retail_raw schemas are preserved (drop manually if needed).")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--profile", default=os.getenv("CZ_PROFILE", "retail_dev"))
    parser.add_argument("--yes", "-y", action="store_true", help="Skip confirmation")
    args = parser.parse_args()
    teardown(args.profile, args.yes)
