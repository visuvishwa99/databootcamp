/*
An incremental query that loads host_activity_reduced day-by-day.
*/

-- delete from  host_activity_reduced;

INSERT INTO host_activity_reduced (month_start_dt, host, metric_name, hit, unique_visitors)
WITH yesterday AS (
    SELECT
        month_start_dt,
        host,
        hit,
        metric_name,
        unique_visitors
    FROM
        host_activity_reduced
    WHERE
        month_start_dt = DATE_TRUNC('month', DATE('2023-01-09')) -- Month start of today's date
),
today_events AS (
    SELECT
        DATE('2023-01-10') AS today_date, -- This date should be dynamic, representing the current day
        host,
        COUNT(1) AS hits,
        COUNT(DISTINCT user_id) AS unique_visitors
    FROM
        events
    WHERE
        DATE(event_time) = DATE('2023-01-10') -- This date should be dynamic
        AND user_id IS NOT NULL
    GROUP BY
        DATE(event_time),
        host
),
today_metrics AS (
    SELECT
        today_date,
        host,
        'hits' AS metric_name,
        hits AS metric_value
    FROM
        today_events
    UNION ALL
    SELECT
        today_date,
        host,
        'unique_visitors' AS metric_name,
        unique_visitors AS metric_value
    FROM
        today_events
),
combined AS (
    SELECT
        COALESCE(y.host, t.host) AS host,
        COALESCE(y.metric_name, t.metric_name) AS metric_name,
        DATE_TRUNC('month', DATE('2023-01-10')) AS month_start_dt, -- Current month start
        EXTRACT(DAY FROM DATE('2023-01-10')) AS day_of_month, -- Current day of month
        y.hit AS yesterday_hits_array,
        y.unique_visitors AS yesterday_unique_visitors_array,
        t.metric_value AS today_metric_value
    FROM
        yesterday y
    FULL OUTER JOIN
        today_metrics t
        ON y.host = t.host AND y.metric_name = t.metric_name
)
SELECT
    month_start_dt,
    host,
    metric_name,
    CASE
        WHEN metric_name = 'hits' THEN
            COALESCE(
                yesterday_hits_array,
                ARRAY_FILL(0, ARRAY[COALESCE(EXTRACT(DAY FROM DATE('2023-01-10')) - 1, 0)::INTEGER])
            ) || ARRAY[COALESCE(today_metric_value, 0)]
        ELSE NULL::BIGINT[] -- If not 'hits' metric, this array should be NULL
    END AS hit,
    CASE
        WHEN metric_name = 'unique_visitors' THEN
            COALESCE(
                yesterday_unique_visitors_array,
                ARRAY_FILL(0, ARRAY[COALESCE(EXTRACT(DAY FROM DATE('2023-01-10')) - 1, 0)::INTEGER])
            ) || ARRAY[COALESCE(today_metric_value, 0)]
        ELSE NULL::BIGINT[] -- If not 'unique_visitors' metric, this array should be NULL
    END AS unique_visitors
FROM
    combined
ON CONFLICT (host, month_start_dt, metric_name) DO UPDATE SET
    hit = EXCLUDED.hit,
    unique_visitors = EXCLUDED.unique_visitors;
