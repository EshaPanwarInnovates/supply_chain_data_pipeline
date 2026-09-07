SELECT carrier, shipments, ROUND(avg_delivery_days::numeric,2) AS avg_delivery_days,
       ROUND(avg_delay_days::numeric,2) AS avg_delay_days, delayed_shipments
FROM delivery_performance ORDER BY delayed_shipments DESC;
