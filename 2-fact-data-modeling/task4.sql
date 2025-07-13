/*
A datelist_int generation query. Convert the device_activity_datelist column into a datelist_int column
*/

-- drop table user_devices_cumulated_int;
CREATE TABLE user_devices_cumulated_int (
    user_id TEXT,
    --browser_type TEXT, -- e.g., 'Chrome', 'Firefox', etc.  
    datelist_int BIT(32),  -- 32 bits for 32 days
    current_user_date DATE,
    PRIMARY KEY (user_id, current_user_date)  -- maps browser_type to array of dates
);


WITH starter AS (
  SELECT
    uc.user_id,
    --uc.browser_type,
    -- flag if this calendar day is in your DATE[]  
    uc.device_activity_datelist @> ARRAY[DATE(d.valid_date)] AS is_active,
    -- days_since = “how many days ago” from your ref date
    EXTRACT(
      DAY FROM DATE '2023-01-31' - d.valid_date
    )::INT AS days_since
  FROM user_devices_cumulated uc
  CROSS JOIN (
    -- 32-day window ending on your current_user_date
    SELECT generate_series(
      '2022-12-31'::DATE,
      '2023-01-31'::DATE,
      INTERVAL '1 day'
    ) AS valid_date
  ) AS d
  WHERE uc.current_user_date = DATE '2023-01-31'
),

bits AS (
  SELECT
    user_id,
    -- browser_type,
    -- set bit31 for today, bit0 for 31 days ago
    SUM(
      CASE
        WHEN is_active 
          THEN POW(2, 31 - days_since)::BIGINT
        ELSE 0
      END
    )::BIGINT::BIT(32) AS datelist_int,
    DATE '2023-01-31' AS current_user_date
  FROM starter
  GROUP BY user_id
  --browser_type
)
INSERT INTO user_devices_cumulated_int (user_id, datelist_int, current_user_date)
SELECT user_id, datelist_int, current_user_date
FROM bits;
--where user_id= '14434123505499000000';
