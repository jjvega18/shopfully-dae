-- Top stores in a period (edit the timestamps)
SELECT
  s.store_id,
  s.store_city,
  s.store_cat,
  COUNT(*) AS visits
FROM "FactVisits" f
JOIN "DimStore" s ON s.store_sk = f.store_sk
WHERE f.visit_ts >= TIMESTAMP '2026-01-15 00:00:00'
  AND f.visit_ts <  TIMESTAMP '2026-01-17 00:00:00'
GROUP BY 1,2,3
ORDER BY visits DESC
LIMIT 10;

-- Top categories in a period
SELECT
  s.store_cat,
  COUNT(*) AS visits
FROM "FactVisits" f
JOIN "DimStore" s ON s.store_sk = f.store_sk
WHERE f.visit_ts >= TIMESTAMP '2026-01-15 00:00:00'
  AND f.visit_ts <  TIMESTAMP '2026-01-17 00:00:00'
GROUP BY 1
ORDER BY visits DESC;

-- Top categories by city (same period)
SELECT
  s.store_city,
  s.store_cat,
  COUNT(*) AS visits
FROM "FactVisits" f
JOIN "DimStore" s ON s.store_sk = f.store_sk
WHERE f.visit_ts >= TIMESTAMP '2026-01-15 00:00:00'
  AND f.visit_ts <  TIMESTAMP '2026-01-17 00:00:00'
GROUP BY 1,2
ORDER BY s.store_city, visits DESC;

-- Visits by city
SELECT
  s.store_city,
  COUNT(*) AS visits
FROM "FactVisits" f
JOIN "DimStore" s ON s.store_sk = f.store_sk
GROUP BY 1
ORDER BY visits DESC;

-- Visits by hour of day (overall)
SELECT
  EXTRACT(HOUR FROM f.visit_ts) AS hour_of_day,
  COUNT(*) AS visits
FROM "FactVisits" f
GROUP BY 1
ORDER BY 1;

-- Visits by hour of day and city
SELECT
  s.store_city,
  EXTRACT(HOUR FROM f.visit_ts) AS hour_of_day,
  COUNT(*) AS visits
FROM "FactVisits" f
JOIN "DimStore" s ON s.store_sk = f.store_sk
GROUP BY 1,2
ORDER BY s.store_city, hour_of_day;

-- Average duration by city (optional behavioral dimension)
SELECT
  s.store_city,
  AVG(f.duration_s) AS avg_duration_s,
  COUNT(*) FILTER (WHERE f.duration_s IS NULL) AS null_duration_rows
FROM "FactVisits" f
JOIN "DimStore" s ON s.store_sk = f.store_sk
GROUP BY 1
ORDER BY avg_duration_s DESC NULLS LAST;

-- Per-user journey summary: frequency, recency, store diversity
SELECT
  u.user_id,
  COUNT(*) AS total_visits,
  MAX(f.visit_ts) AS last_visit_ts,
  MIN(f.visit_ts) AS first_visit_ts,
  COUNT(DISTINCT s.store_id) AS distinct_stores,
  COUNT(DISTINCT s.store_cat) AS distinct_categories
FROM "FactVisits" f
JOIN "DimUser" u  ON u.user_sk  = f.user_sk
JOIN "DimStore" s ON s.store_sk = f.store_sk
GROUP BY 1
ORDER BY total_visits DESC, last_visit_ts DESC;

-- Average journey metrics across users ("typical" user)
SELECT
  AVG(user_stats.total_visits) AS avg_visits_per_user,
  AVG(user_stats.distinct_stores) AS avg_distinct_stores_per_user,
  AVG(user_stats.distinct_categories) AS avg_distinct_categories_per_user,
  AVG(user_stats.active_days) AS avg_active_days_per_user
FROM (
  SELECT
    u.user_id,
    COUNT(*) AS total_visits,
    COUNT(DISTINCT s.store_id) AS distinct_stores,
    COUNT(DISTINCT s.store_cat) AS distinct_categories,
    COUNT(DISTINCT DATE_TRUNC('day', f.visit_ts)) AS active_days
  FROM "FactVisits" f
  JOIN "DimUser" u  ON u.user_sk  = f.user_sk
  JOIN "DimStore" s ON s.store_sk = f.store_sk
  GROUP BY 1
) AS user_stats;

-- Example "journey timeline" for one user: ordered sequence of visits
SELECT
  u.user_id,
  f.visit_ts,
  s.store_id,
  s.store_city,
  s.store_cat,
  f.duration_s
FROM "FactVisits" f
JOIN "DimUser" u  ON u.user_sk  = f.user_sk
JOIN "DimStore" s ON s.store_sk = f.store_sk
WHERE u.user_id = 'u_3310'
ORDER BY f.visit_ts;

-- Time between visits for each user (recency gaps / cadence)
SELECT
  u.user_id,
  f.visit_ts,
  LAG(f.visit_ts) OVER (PARTITION BY u.user_id ORDER BY f.visit_ts) AS prev_visit_ts,
  EXTRACT(EPOCH FROM (f.visit_ts - LAG(f.visit_ts) OVER (PARTITION BY u.user_id ORDER BY f.visit_ts))) / 60.0
    AS minutes_since_prev_visit
FROM "FactVisits" f
JOIN "DimUser" u ON u.user_sk = f.user_sk
ORDER BY u.user_id, f.visit_ts;