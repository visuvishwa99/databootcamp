"""

Cumulative table generation query: Write a query that populates the actors table one year at a time.

"""

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

  (CASE
    WHEN COALESCE(t.avg_rating, 0) > 8 THEN 'star'
    WHEN COALESCE(t.avg_rating, 0) > 7 THEN 'good'
    WHEN COALESCE(t.avg_rating, 0) > 6 THEN 'average'
    ELSE 'bad'
    END )::quality_class AS quality_class,

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

------------------------------------------------------------------------------------------
--store procedure to insert actors data for each year
------------------------------------------------------------------------------------------

DO $$
DECLARE
    start_year INT;
    end_year INT;
    current INT;
BEGIN
    -- Step 1: get year range
    SELECT MIN(year), MAX(year) INTO start_year, end_year FROM actor_films;

    -- Step 2: loop through years one by one
    FOR current IN (start_year + 1)..end_year LOOP

        INSERT INTO actors (
            actorid,
            actor,
            films,
            quality_class,
            is_active,
            current_year
        )
        WITH 
        yesterday AS (
            SELECT *
            FROM actors
            WHERE current_year = current - 1
        ),
        today AS (
            SELECT
                actorid,
                actor,
                year,
                ARRAY_AGG(ROW(film, votes, rating, filmid)::films)
                    OVER (PARTITION BY actorid, actor, year) AS films,
                AVG(rating)
                    OVER (PARTITION BY actorid, actor, year) AS avg_rating,
                ROW_NUMBER()
                    OVER (PARTITION BY actorid ORDER BY actorid) AS rn
            FROM actor_films
            WHERE year = current
        ),
        actor_temp AS (
            SELECT
                COALESCE(y.actorid, t.actorid) AS actorid,
                COALESCE(y.actor,  t.actor) AS actor,
                CASE
                    WHEN y.films IS NULL THEN t.films
                    WHEN t.films IS NULL THEN y.films
                    ELSE y.films || t.films
                END AS films,
                CASE
                    WHEN COALESCE(t.avg_rating, 0) > 8 THEN 'star'
                    WHEN COALESCE(t.avg_rating, 0) > 7 THEN 'good'
                    WHEN COALESCE(t.avg_rating, 0) > 6 THEN 'average'
                    ELSE 'bad'
                END AS quality_class,
                (t.actorid IS NOT NULL) AS is_active,
                COALESCE(t.year, y.current_year + 1) AS current_year
            FROM yesterday y
            FULL OUTER JOIN (
                SELECT actorid, actor, year, films, avg_rating
                FROM today
                WHERE rn = 1
            ) t ON y.actorid = t.actorid
        )
        SELECT * FROM actor_temp;

    END LOOP;
END $$;
