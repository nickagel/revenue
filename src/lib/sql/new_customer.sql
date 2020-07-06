SELECT 
    customer_id,
    DATE_FORMAT(MIN(created_at), 'YYYY-MM') AS year_month 
FROM 
    order_log 
GROUP BY
    customer_id