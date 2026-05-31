# bigquery2lakehouse-retail

[Retail Data Pipeline](https://github.com/alanceloth/Retail_Data_Pipeline) migrated from BigQuery + Airflow + Cosmos + dbt to ClickZetta Lakehouse + Studio Tasks + dbt-clickzetta.

## Repository structure

```
bigquery2lakehouse-retail/
├── 01_bigquery/          # Original project (verbatim copy for comparison)
│   ├── dags/retail.py    # Airflow DAG with Cosmos DbtTaskGroup
│   ├── include/dbt/      # Original dbt-bigquery project
│   └── include/soda/     # Original Soda data quality checks
├── 02_migration/
│   └── MIGRATION_NOTES.md  # Full syntax diff and architecture comparison
├── 03_lakehouse/         # Migrated project — start here
│   ├── README.md         # Step-by-step guide
│   ├── setup.py          # One-shot init: cz-cli profile + dbt profiles.yml
│   ├── requirements.txt
│   ├── e2e.py            # End-to-end verification (11 checks)
│   ├── dbt/              # Migrated dbt project
│   └── tasks/setup.py    # Studio Tasks orchestration (replaces Airflow DAG)
├── data/                 # Raw CSV files
├── .env.example          # Connection template
└── README.md
```

## What's inside

An online retail dataset (541k rows, UK e-commerce 2010) with a full star-schema dbt pipeline:

- 2 raw seed tables (online_retail, country)
- 4 transform models (dim_customer, dim_datetime, dim_product, fct_invoices)
- 3 report models (by customer, by product, by year/month)
- 18 dbt tests (unique, not_null, relationships)

## Quickstart

```bash
git clone https://github.com/clickzetta/bigquery2lakehouse-retail.git
cd bigquery2lakehouse-retail

cp .env.example .env
# edit .env with your ClickZetta instance details

cd 03_lakehouse
pip install -r requirements.txt
python setup.py          # creates cz-cli profile + dbt/profiles.yml

cd dbt
dbt deps --profiles-dir .
dbt seed --profiles-dir .
dbt run --profiles-dir .
dbt test --profiles-dir .

cd ..
python e2e.py            # 11/11 checks
```

Expected output:

```
dbt seed  → Done. PASS=2  WARN=0 ERROR=0 SKIP=0 TOTAL=2
dbt run   → Done. PASS=7  WARN=0 ERROR=0 SKIP=0 TOTAL=7
dbt test  → Done. PASS=18 WARN=0 ERROR=0 SKIP=0 TOTAL=18
e2e       → 11/11 checks passed
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

Full migration guide (Chinese): [DBT BigQuery 迁移实战：零售数仓管道](https://docs.yunqi.tech)

Original project: [alanceloth/Retail_Data_Pipeline](https://github.com/alanceloth/Retail_Data_Pipeline)
