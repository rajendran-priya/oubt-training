WITH
gold AS (
  SELECT *
  FROM "AwsDataCatalog"."day20-curated-master"."taxi_gold_enriched"
),
payment_master AS (
  SELECT *
  FROM "AwsDataCatalog"."day20-curated-master"."payment_type_master_governed"
  WHERE is_current = true
),

-- 1) totals
totals AS (
  SELECT
    'ROW_COUNT' AS section,
    'total_rows' AS metric,
    CAST(count(*) AS double) AS value,
    CAST(NULL AS varchar) AS dim
  FROM gold
),

-- 2) partition distribution
borough_counts AS (
  SELECT
    'PARTITIONS' AS section,
    'rows_by_pickup_borough' AS metric,
    CAST(count(*) AS double) AS value,
    CAST(pickup_borough AS varchar) AS dim
  FROM gold
  GROUP BY pickup_borough
),

-- 3) master current count
master_counts AS (
  SELECT
    'MASTER' AS section,
    'current_payment_type_rows' AS metric,
    CAST(count(*) AS double) AS value,
    CAST(NULL AS varchar) AS dim
  FROM payment_master
),

-- 4) join test: trips by payment type description
join_payment AS (
  SELECT
    'JOIN_TEST' AS section,
    'trips_by_payment_type' AS metric,
    CAST(count(*) AS double) AS value,
    CAST(m.description AS varchar) AS dim
  FROM gold g
  JOIN payment_master m
    ON cast(g.payment_type as varchar) = cast(m.payment_type_id as varchar)

  GROUP BY m.description
),

-- 5) quick DQ summary (optional but very demo-friendly)
dq_checks AS (
  SELECT
    'DQ' AS section,
    'negative_trip_duration_cnt' AS metric,
    CAST(sum(CASE WHEN trip_duration_min < 0 THEN 1 ELSE 0 END) AS double) AS value,
    CAST(NULL AS varchar) AS dim
  FROM gold

  UNION ALL
  SELECT
    'DQ',
    'min_trip_revenue',
    CAST(min(trip_revenue) AS double),
    CAST(NULL AS varchar)
  FROM gold

  UNION ALL
  SELECT
    'DQ',
    'unknown_borough_pct',
    CAST(
      100.0 * sum(CASE WHEN lower(pickup_borough) = 'unknown' THEN 1 ELSE 0 END) / count(*)
      AS double
    ),
    CAST(NULL AS varchar)
  FROM gold
)

SELECT * FROM totals
UNION ALL
SELECT * FROM master_counts
UNION ALL
SELECT * FROM dq_checks
UNION ALL
SELECT * FROM borough_counts
UNION ALL
SELECT * FROM join_payment
ORDER BY
  section,
  metric,
  value DESC;
