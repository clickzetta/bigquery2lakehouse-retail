WITH datetime_cte AS (
    SELECT DISTINCT
        CAST(InvoiceDate AS varchar) AS datetime_id,
        DATE_FORMAT(InvoiceDate, 'yyyy-MM-dd HH:mm:ss') AS date_part
    FROM {{ source('retail', 'raw_invoices') }}
    WHERE InvoiceDate IS NOT NULL
)
SELECT
    datetime_id,
    CAST(date_part AS timestamp) AS datetime,
    SUBSTR(date_part, 9, 2) AS day,
    SUBSTR(date_part, 6, 2) AS month,
    SUBSTR(date_part, 1, 4) AS year,
    SUBSTR(date_part, 12, 2) AS hour,
    SUBSTR(date_part, 15, 2) AS minute,
    DAYOFWEEK(CAST(date_part AS timestamp)) AS weekday
FROM datetime_cte
