SELECT
    u.user_id,
    u.country,
    SUM(f.amount) AS total_revenue
FROM fact_transactions f
JOIN dim_users u
    ON f.user_id = u.user_id
GROUP BY
    u.user_id,
    u.country
ORDER BY total_revenue DESC
LIMIT 3;
