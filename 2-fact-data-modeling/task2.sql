/*
A DDL for an user_devices_cumulated table that has:

a device_activity_datelist which tracks a users active days by browser_type
data type here should look similar to MAP<STRING, ARRAY[DATE]>
or you could have browser_type as a column with multiple rows for each user (either way works, just be consistent!)
*/

-- drop table user_devices_cumulated;
CREATE TABLE user_devices_cumulated (
    user_id TEXT,
    --browser_type TEXT, -- e.g., 'Chrome', 'Firefox', etc.  
    device_activity_datelist date[],
    current_user_date DATE,
    PRIMARY KEY (user_id, current_user_date)  -- maps browser_type to array of dates
);




select * from user_devices_cumulated where current_user_date = date('2023-01-31')
and user_id='14434123505499000000'