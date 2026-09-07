SELECT p.product_id, p.product_name, SUM(p.order_value) AS revenue, SUM(p.quantity) AS units
FROM fact_orders p
GROUP BY p.product_id, p.product_name
ORDER BY revenue DESC
LIMIT 20;
