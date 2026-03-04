SELECT
    COUNT(*) AS total_transactions,
    SUM(amount) AS total_revenue,
    ROUND(SUM(amount) / COUNT(*), 2) AS average_order_value
FROM fact_transactions;
