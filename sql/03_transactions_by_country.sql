SELECT
    u.country,
    COUNT(*) AS transactions,
    SUM(f.amount) AS revenue
FROM fact_transactions f
JOIN dim_users u
    ON f.user_id = u.user_id
GROUP BY u.country
ORDER BY revenue DESC;
