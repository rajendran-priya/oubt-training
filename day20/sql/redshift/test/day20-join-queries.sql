SET search_path TO day20_final_demo;
--Trips by Payment Type
SELECT
  p.description AS payment_type,
  COUNT(*) AS trips
FROM taxi_gold_enriched f
JOIN dim_payment_type p
  ON CAST(f.payment_type AS VARCHAR(50)) = p.payment_type_id
WHERE p.is_current = true
GROUP BY 1
ORDER BY trips DESC;

--Trips by Borough (Zone dimension)
SELECT
  z.borough,
  COUNT(*) AS trips
FROM taxi_gold_enriched f
JOIN dim_taxi_zone z
  ON CAST(f.pulocationid AS VARCHAR(50)) = z.locationid
WHERE z.is_current = true
GROUP BY 1
ORDER BY trips DESC;

--Trips by Vendor

SELECT
  v.name AS vendor,
  COUNT(*) AS trips
FROM taxi_gold_enriched f
JOIN dim_vendor v
  ON CAST(f.vendorid AS VARCHAR(50)) = v.vendor_id
WHERE v.is_current = true
GROUP BY 1
ORDER BY trips DESC;

--Trips by Rate Code

SELECT
  r.description AS rate_code,
  COUNT(*) AS trips
FROM taxi_gold_enriched f
JOIN dim_ratecode r
  ON CAST(f.ratecodeid AS VARCHAR(50)) = r.ratecode_id
WHERE r.is_current = true
GROUP BY 1
ORDER BY trips DESC;


