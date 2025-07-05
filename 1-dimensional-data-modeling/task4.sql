
"""
4.Backfill query for actors_history_scd: Write a "backfill" query that can populate the entire actors_history_scd table in a single query.
"""

-- 2) Backfill in one shot
WITH previous AS (
  SELECT
    actorid    AS actor_id,
    actor,
    quality_class,
    is_active,
    current_year,
    LAG(quality_class) OVER (PARTITION BY actorid ORDER BY current_year) AS previous_quality_class,
    LAG(is_active)      OVER (PARTITION BY actorid ORDER BY current_year) AS previous_is_active
  FROM actors
  WHERE current_year <= 2021
),
change_indicator AS (
  SELECT
    actor_id,
    actor,
    quality_class,
    is_active,
    current_year,
    -- fire on first row OR any real change
    CASE
      WHEN previous_quality_class IS NULL
        OR quality_class    <> previous_quality_class
        OR previous_is_active IS NULL
        OR is_active         <> previous_is_active
      THEN 1
      ELSE 0
    END AS indicator
  FROM previous
),
streaks AS (
  SELECT
    actor_id,
    actor,
    quality_class,
    is_active,
    current_year,
    SUM(indicator) OVER (PARTITION BY actor_id,actor ORDER BY current_year) AS streak_number
  FROM change_indicator
),
grouped_ranges AS (
  SELECT
    actor_id,
    actor,
    quality_class,
    is_active,
    MIN(current_year) AS start_year,
    MAX(current_year) AS end_year,
    2021 AS current_year,
    streak_number
  FROM streaks
  GROUP BY
    actor_id,
    actor,
    quality_class,
    is_active,
    streak_number
),
ranges AS (
  SELECT
    actor_id,
    actor,
    quality_class,
    is_active,
    -- start on Jan 1 of the first year | use DATE_TRUNC if it is string
    MAKE_DATE(start_year, 1, 1)                AS start_date,
    -- end on Dec 31 of the last year
    MAKE_DATE(end_year, 12, 31)                 AS end_date,
    current_year
  FROM grouped_ranges
)
INSERT INTO actors_history_scd (
  actor_id,
  actor,
  start_date,
  end_date,
  quality_class,
  is_active,
  current_year
)
SELECT
  actor_id,
  actor,
  EXTRACT(YEAR FROM start_date) as start_date,
  EXTRACT(YEAR FROM end_date) as end_date,
  quality_class,
  is_active,
  current_year
FROM ranges
ORDER BY actor_id, start_date;
