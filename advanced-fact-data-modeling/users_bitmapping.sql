-- GOAL IS TO REDUCE THE SIZE OF THE ORIGNAL BY +90%

-- we create a intermediate table for the datelist transformation
create or replace table users_cumulated (
  primary key (user_id, log_date),
  user_id double,
  dates_active date[],
  browser_type string[],
  log_date date
);
-- datelist transformation logic of the fact data
INSERT INTO users_cumulated
WITH max_log_date AS (
    SELECT
        MAX(event_time)::DATE AS last_log_date
    FROM events
),
user_activity AS (
    SELECT
        e.user_id,
        ARRAY_AGG(DISTINCT e.event_time::DATE ORDER BY e.event_time::DATE) AS dates_active,
        ARRAY_AGG(DISTINCT d.browser_type ORDER BY d.browser_type) AS browsers_type
    FROM events AS e
    JOIN devices AS d ON e.device_id = d.device_id
    WHERE e.user_id IS NOT NULL
    GROUP BY e.user_id
)
SELECT
    ua.user_id,
    ua.dates_active,
    ua.browsers_type,
    mld.last_log_date AS log_date
FROM user_activity AS ua
CROSS JOIN max_log_date AS mld;

-- final step of bitmapping the datelist
WITH users AS (
	SELECT * FROM users_cumulated
	WHERE log_date = DATE('2023-01-31')
), 
series AS (
  SELECT *
  FROM generate_series(DATE('2023-01-01'), DATE('2023-01-31'), INTERVAL '1 DAY') AS t(series_date)
),
placeholder_ints AS (
  SELECT *, 
  CASE 
  WHEN dates_active @> ARRAY[DATE(series_date)] THEN CAST(POW(2, 32 - (datediff('day', series_date, log_date) + 1)) AS BIGINT)
  ELSE 0 END AS placeholder_int_value, 
  FROM users CROSS JOIN series
)
SELECT
	user_id,
	browser_type,
	CAST(CAST(SUM(placeholder_int_value) AS BIGINT) AS BIT) AS datelist_bit,
	BIT_COUNT(CAST(CAST(SUM(placeholder_int_value) AS BIGINT) AS BIT)) > 0 AS dim_is_monthly_active
FROM placeholder_ints
GROUP BY user_id, browser_type;
