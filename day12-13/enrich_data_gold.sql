/* =========================================================
 * AUTHOR:              Priya Rajendran
 * DATE:                2026-01-11
 * PROJECT:             NYC Taxi Analytics (Hands-on)
 * LAYER:               GOLD (Enrichment & Business Insights)
 * PURPOSE:             Join Silver data with Zone lookup and calculate metrics.
 * DEPENDENCIES:        "day12-sql-transformations"."silver_yellow_taxi"
 * =========================================================
 */

-- Step 1: Create a View for the Gold Layer
CREATE OR REPLACE VIEW "day12-sql-transformations"."enriched_taxi_trips_gold" AS
WITH trip_enrichment AS (
    SELECT 
        t.*,
        -- Calculate Duration in Minutes
        date_diff('minute', t.pickup_time, t.dropoff_time) AS duration_min,
        
        -- Categorize Trip Distance
        CASE 
            WHEN t.trip_distance < 2 THEN 'Short-Haul'
            WHEN t.trip_distance BETWEEN 2 AND 10 THEN 'Medium-Haul'
            ELSE 'Long-Haul' 
        END AS trip_category,
        
        -- Pickup Location Details from Lookup Table
        z_pu.borough AS pickup_borough,
        z_pu.zone AS pickup_zone_name,
        z_pu.service_zone AS pickup_service_area
    FROM 
        "day12-sql-transformations"."silver_yellow_taxi" t
    LEFT JOIN 
        "AwsDataCatalog"."day12-sql-transformations"."taxi_zone_lookup" z_pu 
        ON t.pickup_location_id = z_pu.locationid
)
SELECT 
    pickup_borough,
    trip_category,
    COUNT(*) AS total_trips,
    SUM(total_paid) AS total_revenue,
    AVG(duration_min) AS avg_duration_min,
    AVG(tip_amount) AS avg_tip_amount,
    SUM(airport_fee) AS total_airport_fees,
    CURRENT_DATE AS report_date
FROM 
    trip_enrichment
GROUP BY 
    1, 2
ORDER BY 
    total_revenue DESC;