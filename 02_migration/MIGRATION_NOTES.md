# BigQuery → ClickZetta Lakehouse 迁移说明

## 迁移概览

原项目：[alanceloth/Retail_Data_Pipeline](https://github.com/alanceloth/Retail_Data_Pipeline)

原技术栈：GCS + BigQuery + Airflow (Astronomer) + Cosmos + dbt-bigquery + Soda

迁移后：ClickZetta Volume + ClickZetta Lakehouse + Studio Tasks + dbt-clickzetta

## 架构对比

| 层 | BigQuery 原栈 | ClickZetta 迁移后 |
|----|--------------|-----------------|
| 存储 | Google Cloud Storage (GCS) | dbt seed（内部走 Volume + COPY INTO） |
| 计算/仓库 | BigQuery dataset | ClickZetta schema |
| 编排 | Airflow DAG + Cosmos DbtTaskGroup | Studio Tasks（按依赖编排） |
| 转换 | dbt-bigquery | dbt-clickzetta |
| 数据质量 | Soda checks | dbt test |
| 连接认证 | GCP service account JSON | username + password |

## dbt 语法差异

| 文件 | BigQuery 语法 | ClickZetta 语法 | 说明 |
|------|--------------|----------------|------|
| `dim_datetime.sql` | `CAST(col AS STRING)` | `CAST(col AS varchar)` | ClickZetta 无 STRING 类型 |
| `dim_datetime.sql` | `FORMAT_TIMESTAMP('%Y-%m-%d %H:%M:%S', col)` | `DATE_FORMAT(col, 'yyyy-MM-dd HH:mm:ss')` | 格式化函数不同，格式字符串也不同 |
| `dim_datetime.sql` | `CAST(str AS datetime)` | `CAST(str AS timestamp)` | ClickZetta 无 datetime 类型，用 timestamp |
| `dim_datetime.sql` | `EXTRACT(DAYOFWEEK FROM TIMESTAMP(col))` | `DAYOFWEEK(CAST(col AS timestamp))` | EXTRACT(DAYOFWEEK) 改为函数调用 |
| `fct_invoices.sql` | `CAST(InvoiceDate AS STRING)` | `CAST(InvoiceDate AS varchar)` | 同上 |
| `profiles.yml` | `type: bigquery` + `method: service-account` + `keyfile` | `type: clickzetta` + `username` + `password` | 认证方式不同 |
| `sources.yml` | `database: 'project-id'` | `schema: retail_raw` | BigQuery 用 project+dataset，ClickZetta 用 schema |

## 无需修改的部分

- `dim_customer.sql`：标准 SQL，无平台专有语法
- `dim_product.sql`：标准 SQL，无平台专有语法
- `fct_invoices.sql`：除 CAST AS STRING 外，其余标准 SQL
- `report_*.sql`：全部标准 SQL，零改动
- `dbt_utils.generate_surrogate_key`：dbt_utils 跨平台兼容，无需修改

## Airflow DAG → Studio Tasks 映射

原 Airflow DAG（`retail.py`）的 11 个步骤映射到 Studio Tasks：

| Airflow Task | Studio Task | 说明 |
|-------------|-------------|------|
| `correct_csv_format` | 无需（dbt seed 自动处理） | Airflow 需手动转换日期格式，dbt seed 通过 column_types 直接处理 |
| `upload_retail_csv_to_gcs` | 无需（dbt seed 内置） | GCS 上传步骤消除 |
| `upload_country_csv_to_gcs` | 无需（dbt seed 内置） | GCS 上传步骤消除 |
| `create_retail_dataset` | 无需（dbt seed 自动建表） | BigQuery dataset 创建步骤消除 |
| `retail_gcs_to_raw` | `seed_raw_data`（dbt seed） | GCS→BigQuery 加载改为 dbt seed |
| `country_gcs_to_raw` | `seed_raw_data`（同上） | 合并到同一个 dbt seed 步骤 |
| `check_load`（Soda） | `dbt_test_sources` | Soda schema check 改为 dbt source freshness/test |
| `transform`（Cosmos DbtTaskGroup） | `dbt_run_transform` | 4 个 transform 模型 |
| `check_transform`（Soda） | `dbt_test_transform` | Soda check 改为 dbt test |
| `report`（Cosmos DbtTaskGroup） | `dbt_run_report` | 3 个 report 模型 |
| `check_report`（Soda） | `dbt_test_report` | Soda check 改为 dbt test |

迁移后 Studio Tasks 依赖链：

```
seed_raw_data → dbt_run_transform → dbt_test_transform → dbt_run_report → dbt_test_report
```

## 迁移收益

1. **消除 GCS 依赖**：原项目需要配置 GCS bucket、IAM 权限、service account JSON，迁移后 `dbt seed` 一条命令完成数据加载
2. **消除 Airflow 基础设施**：不再需要 Docker + Astronomer + Cosmos，Studio Tasks 原生支持 dbt 编排
3. **Soda → dbt test**：数据质量检查内置到 dbt，无需额外维护 Soda 配置文件
4. **标准 SQL 零改动**：7 张模型中 5 张无需修改，迁移成本集中在 2 处 BigQuery 专有函数
