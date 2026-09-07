SELECT city, inventory_status, COUNT(*) AS sku_count, SUM(current_stock) AS units
FROM inventory_health
GROUP BY city, inventory_status
ORDER BY city, inventory_status;
