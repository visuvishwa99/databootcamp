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