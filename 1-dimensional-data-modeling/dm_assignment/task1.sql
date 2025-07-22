/*
DDL for actors table: Create a DDL for an actors table with the following fields:
*/


drop type films cascade;

CREATE  TYPE films AS (
  film TEXT,
  votes INT,
  rating FLOAT,
  filmid TEXT
);

drop   TABLE actors cascade;
CREATE  TABLE actors (
  actorid TEXT ,
  actor TEXT,
  films films[],
  quality_class TEXT,
  is_active BOOLEAN,
  current_year INT,
    PRIMARY key (actorid,current_year)
);

-- TRUNCATE table actors;

-- select * from actors;

--validation query 
-- select count(1),actorid,current_year from actors
-- GROUP BY actorid,current_year
-- having count(1)> 1

-- select * from actors_history_scd where actor_id='nm0000001';

-- select max(current_year),min(current_year)  from actors;