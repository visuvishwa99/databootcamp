/*
A DDL for hosts_cumulated table

a host_activity_datelist which logs to see which dates each host is experiencing any activity
*/

drop table hosts_cumulated;
CREATE TABLE hosts_cumulated (
    user_id TEXT,
    host_activity_datelist date[],  -- 32 bits for 32 days
    current_user_date DATE,
    PRIMARY KEY (user_id, current_user_date)  -- maps browser_type to array of dates
);

-- select * from hosts_cumulated;