/*
A cumulative query to generate device_activity_datelist from events
*/


-- select min(DATE(event_time)),max(DATE(event_time)) from events;
-- 2023-01-01	2023-01-31



insert into user_devices_cumulated (user_id, device_activity_datelist, current_user_date)
with yesterday as (

select 
    user_id,
    device_activity_datelist,
    current_user_date
from user_devices_cumulated where current_user_date = DATE('2023-01-03' )

)

,today as (

select 
user_id::TEXT,
DATE(cast(event_time as TIMESTAMP)) as date_active
from events where DATE(cast(event_time as TIMESTAMP)) = DATE('2023-01-04') 
and user_id is not null --filter input data 
GROUP BY user_id, DATE(cast(event_time as TIMESTAMP))

)

select 
COALESCE(y.user_id, t.user_id) as user_id,
case 
    when y.current_user_date is null then ARRAY[t.date_active]
    when t.date_active is null then ARRAY[y.current_user_date]
    else  ARRAY[t.date_active] || y.current_user_date END as date_active,
COALESCE(t.date_active,y.current_user_date + INTERVAL  '1 day') as current_user_date
from today T FULL OUTER JOIN yesterday y
ON t.user_id=y.user_id
;
