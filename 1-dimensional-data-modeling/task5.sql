
"""
Incremental query for actors_history_scd: Write an "incremental" query that combines the previous year's SCD data with new incoming data from the actors table.
"""

-- drop TYPE scd_type CASCADE;
-- create type scd_type as (

--     quality_class text,
--     is_active boolean,
--     start_date int,
--     end_date int
-- );

with last_year_scd as (
    select 
        actor_id,
        actor,
        quality_class,
        is_active,
        start_date,
        end_date,
        current_year
    from actors_history_scd
    where current_year = 2021
    and end_date = 2021

),

historical_scd as (
    select 
        actor_id,
        actor,
        quality_class,
        is_active,
        --current_year,
        start_date,
        end_date
    from actors_history_scd
    where current_year = 2021
    and end_date < 2021
),

this_year as (
    select 
        actor_id,
        actor,
        quality_class,
        is_active,
        start_date,
        end_date,
        current_year
    from actors_history_scd
    where current_year = 2021
)

,unchanges_records as (
    select 
        ty.actor_id,
        ty.actor,
        ty.quality_class,
        ty.is_active,
        ly.start_date,
        ty.current_year as end_date
    from this_year ty
    left join last_year_scd ly
    on (ty.actor_id = ly.actor_id)
    where (ly.quality_class = ty.quality_class and ly.is_active = ty.is_active)
)
,changes_records as (
    select 
        ty.actor_id,
        ty.actor,
        unnest(
        ARRAY[
                row(
                    ly.quality_class,
                    ly.is_active,
                    ly.start_date,
                    ly.end_date
                )::scd_type,
            row(
                    ty.quality_class,
                    ty.is_active,
                    ty.current_year,
                    ty.current_year
                )::scd_type
        ]) as records
    from this_year ty
    left join last_year_scd ly
    on (ty.actor_id = ly.actor_id)
    where (ly.quality_class <> ty.quality_class or ly.is_active <> ty.is_active or ly.actor_id is null)
    
    )

,unnested_changed_records as (
   select 
   actor_id,
        actor,
        (records::scd_type).quality_class,
        (records::scd_type).is_active,
        (records::scd_type).start_date, 
        (records::scd_type).end_date
    from changes_records
)
,new_records as (
select 
ty.actor_id,
ty.actor,
ty.quality_class,
ty.is_active,
ty.current_year as start_date,
ty.current_year as end_date
 from this_year ty left join last_year_scd ly on (ty.actor_id = ly.actor_id) where ty.actor_id is null)
select * from historical_scd
union ALL
select * from unnested_changed_records
union ALL
select * from new_records;