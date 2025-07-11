/* A query to deduplicate game_details from Day 1 so theres no duplicates */

with game_details_dedup_flag as (
    select 
    gd.* ,
row_number() OVER(PARTITION BY gd.*) as row_num
from game_details gd
) 
,game_details_dedup
as (
select * from game_details_dedup_flag
where row_num=1
)
select * from game_details_dedup --245781 ;








