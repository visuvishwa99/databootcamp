-- 1) Drop & re-create your history table
"""
3.DDL for actors_history_scd table: Create a DDL for an actors_history_scd table with the following features:

Implements type 2 dimension modeling (i.e., includes start_date and end_date fields).
Tracks quality_class and is_active status for each actor in the actors table.
"""

DROP TABLE IF EXISTS actors_history_scd CASCADE;
CREATE TABLE actors_history_scd (
  actor_id      TEXT    NOT NULL,
  actor         TEXT    NOT NULL,
  start_date    INT    NOT NULL,
  end_date      INT,
  quality_class TEXT    NOT NULL,
  is_active     BOOLEAN,
  current_year  INT     NOT NULL,
  PRIMARY KEY(actor_id, start_date)
);
