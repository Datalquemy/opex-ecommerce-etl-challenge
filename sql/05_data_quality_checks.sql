SELECT
    COUNT(*) AS total_transactions,
    SUM(CASE WHEN amount IS NULL THEN 1 ELSE 0 END) AS null_amounts,
    SUM(CASE WHEN amount <= 0 THEN 1 ELSE 0 END) AS invalid_amounts
FROM fact_transactions;
