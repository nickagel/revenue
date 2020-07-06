SELECT
    DATE_FORMAT(ol.created_at, 'YYYY-MM') AS year_month,
    CAST(SUM(ol.amount) AS DECIMAL(17,2)) AS revenue,
    CAST(100 * (SUM(
        CASE
            WHEN 
                nc.year_month IS NOT NULL
            THEN
                ol.amount
            ELSE
                0
        END
    )/SUM(ol.amount)) AS DECIMAL(5,2)) AS new_revenue_percentage,
    CAST(SUM(
        CASE
            WHEN 
                nc.year_month IS NOT NULL
            THEN
                ol.amount
            ELSE
                0
        END
    ) AS DECIMAL(17,2)) AS revenue_from_new_customers
FROM
    order_log ol
LEFT JOIN
    new_customer nc
ON
    DATE_FORMAT(ol.created_at, 'YYYY-MM') = nc.year_month
AND
    ol.customer_id = nc.customer_id
GROUP BY
    DATE_FORMAT(ol.created_at, 'YYYY-MM')
ORDER BY
    year_month 
ASC