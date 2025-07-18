/*
A DDL for host_activity_reduced table
*/

DROP TABLE IF EXISTS host_activity_reduced;
CREATE TABLE host_activity_reduced (
    month_start_dt DATE,
    host TEXT,
    hit BIGINT[],
    metric_name TEXT, -- This will store the name of the metric like 'hits' or 'unique_visitors'
    unique_visitors  BIGINT[], -- Assuming this will store an array of metrics like hits over time
    
    PRIMARY KEY (host, month_start_dt,metric_name )
);