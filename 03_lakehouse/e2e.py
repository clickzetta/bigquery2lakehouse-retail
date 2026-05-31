#!/usr/bin/env python3
"""
End-to-end verification for bigquery2lakehouse-retail.

Runs assertions against the ClickZetta Lakehouse tables built by dbt.
All EXPECTED values come from actual dbt run output — not guessed.

Usage:
  python e2e.py --profile aliyun_shanghai_prod
"""
import subprocess
import json
import sys
import argparse

EXPECTED = {
    "dim_customer_count": 425,
    "dim_datetime_count": 604,
    "dim_product_count": 3792,
    "fct_invoices_count": 10178,
    "top_country": "United Kingdom",
    "top_country_revenue": 178690.92,
    "top_product_stock_code": "84077",
    "top_product_qty": 2880,
    "year_min": "2010",
    "year_max": "2010",
    "total_revenue": 197573.37,
}


def sql(query, profile):
    result = subprocess.run(
        ["cz-cli", "sql", query, "--profile", profile, "--sync"],
        capture_output=True, text=True
    )
    data = json.loads(result.stdout)
    if "error" in data:
        raise RuntimeError(data["error"]["message"])
    return data["rows"]


def check(name, actual, expected, tolerance=0.01):
    if isinstance(expected, float):
        ok = abs(actual - expected) <= tolerance
    else:
        ok = actual == expected
    status = "PASS" if ok else "FAIL"
    print(f"  [{status}] {name}: {actual!r} (expected {expected!r})")
    return ok


def run(profile):
    schema = "quick_start.retail"
    passed = 0
    failed = 0

    print(f"\n=== e2e verification (profile: {profile}) ===\n")

    checks = []

    rows = sql(f"SELECT COUNT(*) FROM {schema}.dim_customer", profile)
    checks.append(("dim_customer row count", rows[0][0], EXPECTED["dim_customer_count"]))

    rows = sql(f"SELECT COUNT(*) FROM {schema}.dim_datetime", profile)
    checks.append(("dim_datetime row count", rows[0][0], EXPECTED["dim_datetime_count"]))

    rows = sql(f"SELECT COUNT(*) FROM {schema}.dim_product", profile)
    checks.append(("dim_product row count", rows[0][0], EXPECTED["dim_product_count"]))

    rows = sql(f"SELECT COUNT(*) FROM {schema}.fct_invoices", profile)
    checks.append(("fct_invoices row count", rows[0][0], EXPECTED["fct_invoices_count"]))

    rows = sql(f"SELECT country, ROUND(total_revenue, 2) FROM {schema}.report_customer_invoices ORDER BY total_revenue DESC LIMIT 1", profile)
    checks.append(("top country by revenue", rows[0][0], EXPECTED["top_country"]))
    checks.append(("top country revenue", rows[0][1], EXPECTED["top_country_revenue"]))

    rows = sql(f"SELECT stock_code, total_quantity_sold FROM {schema}.report_product_invoices ORDER BY total_quantity_sold DESC LIMIT 1", profile)
    checks.append(("top product stock_code", rows[0][0], EXPECTED["top_product_stock_code"]))
    checks.append(("top product qty sold", rows[0][1], EXPECTED["top_product_qty"]))

    rows = sql(f"SELECT MIN(year), MAX(year) FROM {schema}.report_year_invoices", profile)
    checks.append(("year range min", rows[0][0], EXPECTED["year_min"]))
    checks.append(("year range max", rows[0][1], EXPECTED["year_max"]))

    rows = sql(f"SELECT ROUND(SUM(total), 2) FROM {schema}.fct_invoices", profile)
    checks.append(("total revenue", rows[0][0], EXPECTED["total_revenue"]))

    for name, actual, expected in checks:
        if check(name, actual, expected):
            passed += 1
        else:
            failed += 1

    print(f"\n{'='*40}")
    print(f"Result: {passed}/{passed+failed} checks passed")
    if failed > 0:
        print("FAILED")
        sys.exit(1)
    else:
        print("ALL PASSED")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--profile", default="aliyun_shanghai_prod")
    args = parser.parse_args()
    run(args.profile)
