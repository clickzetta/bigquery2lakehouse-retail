# 03_lakehouse — ClickZetta Lakehouse 迁移后项目

本目录是 [alanceloth/Retail_Data_Pipeline](https://github.com/alanceloth/Retail_Data_Pipeline) 迁移到 ClickZetta Lakehouse 后的完整项目。

原始项目（BigQuery + Airflow + Cosmos + Soda）在 `../01_bigquery/`，迁移说明在 `../02_migration/MIGRATION_NOTES.md`。

## 目录结构

```
03_lakehouse/
├── setup.py          # 第一步：初始化连接、生成 dbt profiles.yml
├── requirements.txt  # Python 依赖
├── e2e.py            # 端到端验证（11 项检查）
├── dbt/              # dbt 项目（迁移后）
│   ├── seeds/        # 原始 CSV 数据（替代 GCS + BigQuery load）
│   ├── models/
│   │   ├── sources/  # 数据源定义
│   │   ├── transform/ # 星型模型（dim_* + fct_invoices）
│   │   └── report/   # 聚合报表
│   └── profiles.yml.example
└── tasks/
    └── setup.py      # Studio Tasks 编排（替代 Airflow DAG）
```

## 快速开始

**第一步：配置连接**

```bash
cd bigquery2lakehouse-retail
cp .env.example .env
# 编辑 .env，填入你的 ClickZetta 实例信息
```

`.env` 需要填写的字段：

```
CLICKZETTA_INSTANCE=<your-instance-id>
CLICKZETTA_WORKSPACE=<your-workspace>
CLICKZETTA_USERNAME=<your-username>
CLICKZETTA_PASSWORD=<your-password>
CLICKZETTA_VCLUSTER=default_ap
CZ_PROFILE=retail_dev
```

**第二步：初始化**

```bash
cd 03_lakehouse
pip install -r requirements.txt
python setup.py
```

`setup.py` 会自动创建 cz-cli profile 并生成 `dbt/profiles.yml`。

**第三步：运行 dbt 管道**

```bash
cd dbt
dbt deps --profiles-dir .
dbt seed --profiles-dir .    # 加载原始数据（替代 GCS + BigQuery load）
dbt run --profiles-dir .     # 构建星型模型
dbt test --profiles-dir .    # 数据质量检查（替代 Soda）
```

预期输出：

```
dbt seed  → Done. PASS=2  WARN=0 ERROR=0 SKIP=0 TOTAL=2
dbt run   → Done. PASS=7  WARN=0 ERROR=0 SKIP=0 TOTAL=7
dbt test  → Done. PASS=18 WARN=0 ERROR=0 SKIP=0 TOTAL=18
```

**第四步：端到端验证**

```bash
cd ..
python e2e.py
```

预期输出：

```
Result: 11/11 checks passed
ALL PASSED
```

**第五步：配置 Studio Tasks 编排（可选）**

替代原项目的 Airflow DAG，在 ClickZetta Studio 中创建任务依赖链：

```bash
python tasks/setup.py
```

创建的任务依赖链：

```
retail_seed_raw_data
  → retail_dbt_run_transform
    → retail_dbt_test_transform
      → retail_dbt_run_report
        → retail_dbt_test_report
```

## 与原项目的对比

| 原项目（BigQuery） | 迁移后（ClickZetta） |
|-------------------|---------------------|
| GCS bucket + IAM + service account | 无需配置，dbt seed 内置 |
| Airflow DAG（11 个 Task） | Studio Tasks（5 个 Task） |
| Cosmos DbtTaskGroup | 直接 `dbt run --select` |
| Soda 数据质量检查 | dbt test |
| `FORMAT_TIMESTAMP('%Y-%m-%d', col)` | `DATE_FORMAT(col, 'yyyy-MM-dd')` |
| `CAST(col AS STRING)` | `CAST(col AS varchar)` |
| `EXTRACT(DAYOFWEEK FROM col)` | `DAYOFWEEK(col)` |

详细迁移说明见 [../02_migration/MIGRATION_NOTES.md](../02_migration/MIGRATION_NOTES.md)。
