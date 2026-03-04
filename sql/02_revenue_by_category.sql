SELECT
    product_category,
    COUNT(*) AS transactions,
    SUM(amount) AS revenue
FROM fact_transactions
GROUP BY product_category
ORDER BY revenue DESC;
