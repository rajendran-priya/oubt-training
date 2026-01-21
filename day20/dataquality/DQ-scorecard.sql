WITH latest AS (
  SELECT dataset, max(run_ts) AS max_run_ts
  FROM "AwsDataCatalog"."day20-data-quality"."dq_scorecard"
  WHERE run_date = cast(current_date AS varchar)
  GROUP BY dataset
)
SELECT d.*
FROM "AwsDataCatalog"."day20-data-quality"."dq_scorecard" d
JOIN latest l
  ON d.dataset = l.dataset
 AND d.run_ts  = l.max_run_ts
WHERE d.run_date = cast(current_date AS varchar)
ORDER BY d.dataset, d.check_name;