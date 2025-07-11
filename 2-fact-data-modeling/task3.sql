/*
A cumulative query to generate device_activity_datelist from events
*/
-- INSERT INTO user_devices_cumulated (user_id, browser_type, device_activity_datelist)

insert into user_devices_cumulated (user_id, browser_type, device_activity_datelist)
SELECT 
    e.user_id,
    d.browser_type,
    ARRAY_AGG(to_date(e.event_time,'YYYY-MM-DD')) AS event_date
FROM events e
JOIN devices d ON e.device_id = d.device_id
GROUP BY e.user_id, d.browser_type;

-- select * from user_devices_cumulated where user_id='9369270486289640000';
