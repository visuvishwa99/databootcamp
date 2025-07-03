WITH yesterday AS (
  SELECT *
  FROM actors
  WHERE current_year = 1970
),

-- Aggregate to grain of year 
today AS (
  SELECT
    actorid,
    actor,
    year,

    ARRAY_AGG(ROW(film, votes, rating, filmid)::films)
      OVER (PARTITION BY actorid, actor, year)            AS films,

    AVG(rating) 
      OVER (PARTITION BY actorid, actor, year)            AS avg_rating,

    ROW_NUMBER() 
      OVER (PARTITION BY actorid ORDER BY actorid)        AS rn
  FROM actor_films
  WHERE year = 1971
)

,
actor_temp as (

SELECT
  COALESCE(y.actorid, t.actorid)    AS actor_id,
  COALESCE(y.actor,  t.actor)       AS actor,

  CASE
    WHEN y.films IS NULL THEN t.films
    WHEN t.films IS NULL THEN y.films
    ELSE y.films || t.films
  END                                 AS films,

  CASE
    WHEN COALESCE(t.avg_rating, 0) > 8 THEN 'star'
    WHEN COALESCE(t.avg_rating, 0) > 7 THEN 'good'
    WHEN COALESCE(t.avg_rating, 0) > 6 THEN 'average'
    ELSE 'bad'
  END                                 AS quality_class,

  (t.actorid IS NOT NULL)            AS is_active,

  COALESCE(t.year, y.current_year + 1)  AS current_year

FROM yesterday y
FULL OUTER JOIN (

  SELECT actorid, actor, year, films, avg_rating
  FROM today
  WHERE rn = 1
) t
  ON y.actorid = t.actorid


)

INSERT INTO actors (
  actorid,
  actor,
  films,
  quality_class,
  is_active,
  current_year
)
SELECT * FROM actor_temp;
