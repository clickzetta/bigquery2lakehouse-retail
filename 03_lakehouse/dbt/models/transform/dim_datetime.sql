WITH datetime_cte AS (
    SELECT DISTINCT
        InvoiceDate AS datetime_id,
        TO_TIMESTAMP(
            REGEXP_REPLACE(InvoiceDate, '(\\d+)/(\\d+)/(\\d+) (\\d+):(\\d+)', '20$3-$1-$2 $4:$5'),
            'yyyy-M-d H:mm'
        ) AS ts
    FROM {{ source('retail', 'online_retail') }}
    WHERE InvoiceDate IS NOT NULL
)
SELECT
    datetime_id,
    ts AS datetime,
    DATE_FORMAT(ts, 'dd') AS day,
    DATE_FORMAT(ts, 'MM') AS month,
    DATE_FORMAT(ts, 'yyyy') AS year,
    DATE_FORMAT(ts, 'HH') AS hour,
    DATE_FORMAT(ts, 'mm') AS minute,
    DAYOFWEEK(ts) AS weekday
FROM datetime_cte
