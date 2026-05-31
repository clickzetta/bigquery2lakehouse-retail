#!/usr/bin/env python3
"""
One-shot setup for bigquery2lakehouse-retail on ClickZetta Lakehouse.

What this does:
  1. Reads connection info from .env (copy from ../.env.example)
  2. Creates a cz-cli profile for subsequent commands
  3. Verifies the connection with a test query
  4. Prints next steps

Usage:
  cd 03_lakehouse
  cp ../.env.example ../.env
  # edit ../.env with your instance details
  python setup.py
"""
import os
import sys
import subprocess
from pathlib import Path
from dotenv import load_dotenv

ENV_FILE = Path(__file__).parent.parent / ".env"


def run(cmd, check=True, capture=True):
    result = subprocess.run(cmd, capture_output=capture, text=True)
    if check and result.returncode != 0:
        print(f"ERROR: {result.stderr.strip() or result.stdout.strip()}")
        sys.exit(1)
    return result.stdout.strip()


def main():
    if not ENV_FILE.exists():
        print(f"ERROR: {ENV_FILE} not found.")
        print(f"  cp .env.example .env  # then fill in your credentials")
        sys.exit(1)

    load_dotenv(ENV_FILE)

    required = ["CLICKZETTA_INSTANCE", "CLICKZETTA_WORKSPACE",
                "CLICKZETTA_USERNAME", "CLICKZETTA_PASSWORD"]
    missing = [k for k in required if not os.getenv(k)]
    if missing:
        print(f"ERROR: missing in .env: {', '.join(missing)}")
        sys.exit(1)

    instance  = os.getenv("CLICKZETTA_INSTANCE")
    workspace = os.getenv("CLICKZETTA_WORKSPACE")
    username  = os.getenv("CLICKZETTA_USERNAME")
    password  = os.getenv("CLICKZETTA_PASSWORD")
    vcluster  = os.getenv("CLICKZETTA_VCLUSTER", "default")
    schema    = os.getenv("CLICKZETTA_SCHEMA", "retail")
    profile   = os.getenv("CZ_PROFILE", "retail_dev")
    service   = os.getenv("CLICKZETTA_SERVICE", "cn-shanghai-alicloud.api.clickzetta.com")

    print(f"
=== bigquery2lakehouse-retail setup ===
")
    print(f"Instance:  {instance}")
    print(f"Workspace: {workspace}")
    print(f"VCluster:  {vcluster}")
    print(f"Schema:    {schema}")
    print(f"Profile:   {profile}
")

    # Create cz-cli profile
    print("[1/3] Creating cz-cli profile...")
    run(["cz-cli", "profile", "create", profile,
         "--service",   service,
         "--instance",  instance,
         "--workspace", workspace,
         "--schema",    schema,
         "--vcluster",  vcluster,
         "--username",  username,
         "--password",  password], check=False)
    print(f"      profile '{profile}' ready")

    # Verify connection
    print("[2/3] Verifying connection...")
    out = run(["cz-cli", "sql", "SELECT 1", "--profile", profile, "--sync"])
    if '"rows":[[1]]' not in out:
        print(f"ERROR: connection test failed: {out}")
        sys.exit(1)
    print("      connection OK")

    # Write profiles.yml for dbt
    print("[3/3] Writing dbt/profiles.yml...")
    profiles_path = Path(__file__).parent / "dbt" / "profiles.yml"
    profiles_content = f"""retail:
  target: dev
  outputs:
    dev:
      type: clickzetta
      service: {service}
      instance: {instance}
      workspace: {workspace}
      username: {username}
      password: {password}
      schema: {schema}
      vcluster: {vcluster}
"""
    profiles_path.write_text(profiles_content)
    print(f"      written to {profiles_path}")

    print(f"""
=== Setup complete ===

Next steps:

  cd 03_lakehouse/dbt
  pip install -r ../requirements.txt
  dbt deps --profiles-dir .
  dbt seed --profiles-dir .
  dbt run --profiles-dir .
  dbt test --profiles-dir .

  # Verify end-to-end
  cd ..
  python e2e.py --profile {profile}

  # Set up Studio Tasks (optional)
  python tasks/setup.py --profile {profile}
""")


if __name__ == "__main__":
    main()
