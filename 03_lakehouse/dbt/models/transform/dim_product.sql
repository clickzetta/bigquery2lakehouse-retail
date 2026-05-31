SELECT DISTINCT
    {{ dbt_utils.generate_surrogate_key(['StockCode', 'Description', 'UnitPrice']) }} AS product_id,
    StockCode AS stock_code,
    Description AS description,
    UnitPrice AS price
FROM {{ source('retail', 'online_retail') }}
WHERE StockCode IS NOT NULL
    AND Description IS NOT NULL
    AND UnitPrice > 0
