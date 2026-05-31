#!/usr/bin/env python3
"""
Setup Studio Tasks for the retail dbt pipeline.

Creates 5 tasks with dependency chain:
  seed_raw_data → dbt_run_transform → dbt_test_transform → dbt_run_report → dbt_test_report

Usage:
  python setup.py --profile retail_dev
"""
import subprocess
import json
import sys
import argparse


TASKS = [
    "retail_seed_raw_data",
    "retail_dbt_run_transform",
    "retail_dbt_test_transform",
    "retail_dbt_run_report",
    "retail_dbt_test_report",
]

TASK_CONTENTS = {
    "retail_seed_raw_data": "dbt seed --profiles-dir /path/to/03_lakehouse/dbt --profiles-dir .",
    "retail_dbt_run_transform": "dbt run --select transform --profiles-dir .",
    "retail_dbt_test_transform": "dbt test --select transform --profiles-dir .",
    "retail_dbt_run_report": "dbt run --select report --profiles-dir .",
    "retail_dbt_test_report": "dbt test --select report --profiles-dir .",
}

# Dependency chain: each task depends on the previous one
DEPS = {
    "retail_dbt_run_transform": "retail_seed_raw_data",
    "retail_dbt_test_transform": "retail_dbt_run_transform",
    "retail_dbt_run_report": "retail_dbt_test_transform",
    "retail_dbt_test_report": "retail_dbt_run_report",
}


def run(cmd, check=True):
    print(f"  $ {' '.join(cmd)}")
    result = subprocess.run(cmd, capture_output=True, text=True)
    if check and result.returncode != 0:
        print(f"  ERROR: {result.stderr.strip()}")
        sys.exit(1)
    return result.stdout.strip()


def get_task_id(task_name, profile):
    out = run(["cz-cli", "task", "list", "--profile", profile, "--format", "json"], check=False)
    try:
        tasks = json.loads(out)
        for t in tasks:
            if t.get("taskName") == task_name:
                return t.get("taskId")
    except Exception:
        pass
    return None


def setup(profile):
    print(f"\n=== Creating Studio Tasks (profile: {profile}) ===\n")

    task_ids = {}

    for task_name in TASKS:
        print(f"[1/3] Creating task: {task_name}")
        run(["cz-cli", "task", "create", task_name, "--profile", profile])

        task_id = get_task_id(task_name, profile)
        if task_id:
            task_ids[task_name] = task_id
            print(f"      task_id: {task_id}")

    print()
    for task_name, content in TASK_CONTENTS.items():
        print(f"[2/3] Setting content: {task_name}")
        run(["cz-cli", "task", "save-content", task_name,
             "--content", content, "--profile", profile])

    print()
    for task_name, upstream_name in DEPS.items():
        upstream_id = task_ids.get(upstream_name)
        if not upstream_id:
            print(f"  WARN: could not find task_id for {upstream_name}, skipping dep")
            continue
        print(f"[3/3] Setting dep: {task_name} → {upstream_name} (id={upstream_id})")
        dep_json = json.dumps([{"taskId": upstream_id, "taskName": upstream_name}])
        run(["cz-cli", "task", "save-config", task_name,
             "--deps", "replace", "--dep-tasks", dep_json, "--profile", profile])

    print("\n=== Setup complete ===")
    print("\nTask dependency chain:")
    print("  retail_seed_raw_data")
    print("    → retail_dbt_run_transform")
    print("      → retail_dbt_test_transform")
    print("        → retail_dbt_run_report")
    print("          → retail_dbt_test_report")
    print("\nTo deploy and run:")
    for task_name in TASKS:
        print(f"  cz-cli task deploy {task_name} --profile {profile}")
    print(f"  cz-cli task execute retail_seed_raw_data --profile {profile}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Setup Studio Tasks for retail dbt pipeline")
    parser.add_argument("--profile", default="retail_dev", help="cz-cli profile name")
    args = parser.parse_args()
    setup(args.profile)
