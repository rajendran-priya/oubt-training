SET search_path TO day20_final_demo;
--Revenue by payment type
SELECT
  p.description AS payment_type,
  SUM(f.trip_revenue) AS total_revenue
FROM taxi_gold_enriched f
JOIN dim_payment_type p
  ON CAST(f.payment_type AS VARCHAR(50)) = p.payment_type_id
WHERE p.is_current = true
GROUP BY 1
ORDER BY total_revenue DESC;


--Average trip duration by borough

SELECT
  z.borough,
  AVG(f.trip_duration_min) AS avg_trip_duration
FROM taxi_gold_enriched f
JOIN dim_taxi_zone z
  ON CAST(f.pulocationid AS VARCHAR(50)) = z.locationid
WHERE z.is_current = true
GROUP BY 1
ORDER BY avg_trip_duration DESC;
