# bigquery2lakehouse-retail

[Retail Data Pipeline](https://github.com/alanceloth/Retail_Data_Pipeline) migrated from BigQuery + Airflow + Cosmos + dbt to ClickZetta Lakehouse + Studio Tasks + dbt-clickzetta.

## What's inside

An online retail dataset (541k rows, UK e-commerce) with a full star-schema dbt pipeline:

- 2 raw seed tables (online_retail, country)
- 4 transform models (dim_customer, dim_datetime, dim_product, fct_invoices)
- 3 report models (by customer, by product, by year/month)

## Prerequisites

- Python 3.10+
- A ClickZetta Lakehouse instance
- dbt-clickzetta 1.6.5+

## Quickstart

```bash
cd 03_lakehouse/dbt
cp profiles.yml.example profiles.yml
# edit profiles.yml with your instance details

python3 -m venv .venv
source .venv/bin/activate
pip install dbt-clickzetta

dbt deps
dbt seed --profiles-dir .
dbt run --profiles-dir .
dbt test --profiles-dir .
```

Expected output:

```
dbt seed  → Done. PASS=2  WARN=0 ERROR=0 SKIP=0 TOTAL=2
dbt run   → Done. PASS=7  WARN=0 ERROR=0 SKIP=0 TOTAL=7
dbt test  → Done. PASS=...
```

## Migration highlights

| Layer | BigQuery stack | ClickZetta stack |
|-------|---------------|-----------------|
| Storage | GCS bucket | dbt seed (Volume + COPY INTO) |
| Warehouse | BigQuery dataset | ClickZetta schema |
| Orchestration | Airflow + Cosmos | Studio Tasks |
| Transform | dbt-bigquery | dbt-clickzetta |
| Data quality | Soda checks | dbt test |
| Auth | service account JSON | username + password |

Key SQL changes: 2 files modified out of 7 models. See [02_migration/MIGRATION_NOTES.md](02_migration/MIGRATION_NOTES.md) for the full diff.

## Documentation

Full migration guide (Chinese): [dbt BigQuery 迁移实战](https://docs.yunqi.tech)

Original project: [alanceloth/Retail_Data_Pipeline](https://github.com/alanceloth/Retail_Data_Pipeline)
