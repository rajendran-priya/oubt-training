SET search_path TO day17_SCD;
INSERT INTO date_dim (full_date, year, quarter, month, day, day_of_week, week_of_year)
WITH RECURSIVE date_series(date_val) AS (
  SELECT '2025-01-01'::DATE
  UNION ALL
  SELECT (date_val + 1)::DATE
  FROM date_series
  WHERE date_val < '2025-12-31'::DATE
)
SELECT 
    date_val,
    EXTRACT(YEAR FROM date_val),
    EXTRACT(QUARTER FROM date_val),
    EXTRACT(MONTH FROM date_val),
    EXTRACT(DAY FROM date_val),
    EXTRACT(DOW FROM date_val), -- 0 = Sunday, 6 = Saturday
    EXTRACT(WEEK FROM date_val)
FROM date_series;