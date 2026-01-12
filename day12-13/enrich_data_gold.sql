/* =========================================================
 * AUTHOR:              Priya Rajendran
 * DATE:                2026-01-12
 * PROJECT:             NYC Taxi Analytics (Hands-on)
 * LAYER:               GOLD (Enrichment & Business Insights)
 * PURPOSE:             Create physical Gold table in S3.
 * =========================================================
 */

CREATE TABLE "day12-sql-transformations"."enriched_taxi_trips_gold_table"
WITH (
  format = 'PARQUET',
  external_location = 's3://day12-sql-transformations/enriched_gold/',
  parquet_compression = 'SNAPPY'
) AS
-- Use a WITH clause (CTE) to prepare the data first
WITH trip_enrichment AS (
    SELECT 
        t.total_paid,
        t.tip_amount,
        -- Calculate Duration
        date_diff('minute', CAST(t.pickup_time AS TIMESTAMP), CAST(t.dropoff_time AS TIMESTAMP)) AS duration_min,
        
        -- Time of Day Categorization
        CASE 
            WHEN hour(CAST(t.pickup_time AS TIMESTAMP)) BETWEEN 6 AND 10 THEN 'Morning Rush'
            WHEN hour(CAST(t.pickup_time AS TIMESTAMP)) BETWEEN 16 AND 20 THEN 'Evening Rush'
            WHEN hour(CAST(t.pickup_time AS TIMESTAMP)) >= 23 OR hour(CAST(t.pickup_time AS TIMESTAMP)) < 5 THEN 'Overnight'
            ELSE 'Mid-Day'
        END AS shift_type,
        
        z_pu.borough AS pickup_borough,
        z_do.borough AS dropoff_borough
    FROM 
        "day12-sql-transformations"."silver_yellow_taxi" t
    LEFT JOIN 
        "day12-sql-transformations"."master_data" z_pu 
        ON t.pickup_zone = z_pu.locationid
    LEFT JOIN 
        "day12-sql-transformations"."master_data" z_do 
        ON t.dropoff_zone = z_do.locationid
)
-- Now perform the final aggregation for the physical table
SELECT 
    pickup_borough,
    dropoff_borough,
    shift_type,
    COUNT(*) AS total_trips,
    ROUND(SUM(total_paid), 2) AS total_revenue,
    ROUND(AVG(duration_min), 2) AS avg_duration_min,
    ROUND(SUM(total_paid) / NULLIF(SUM(duration_min), 0), 2) AS rev_per_minute
FROM 
    trip_enrichment
GROUP BY 
    1, 2, 3
ORDER BY 
    total_revenue DESC;