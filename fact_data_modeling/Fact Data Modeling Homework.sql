--- Question 1
select * from game_details;


with deduped as (
select g.game_date_est,
	   g.season,
	   g.home_team_id,
	   gd.*,
	   row_number() over (partition by gd.game_id, team_id, player_id) as row_num
from game_details gd 
join games g 
on gd.game_id = g.game_id
)
select game_date_est as dim_game_date,
	   season as dim_season,
	   team_id as dim_team_id,
	   player_id as dim_player_id,
	   player_name as dim_player_name,
	   start_position as dim_start_position,
	   team_id = home_team_id as dim_is_playing_at_home,
	   coalesce (position('DNP' in comment), 0) > 0 as dim_did_not_play,
	   coalesce (position('DND' in comment), 0) > 0 as dim_did_not_dress,
	   coalesce (position('NWT' in comment), 0) > 0 as dim_not_with_team,
	   cast (split_part(min, ':', 1) as real) + cast(split_part(min, ':', 2) as real)/60
	   as m_minutes ,
	   fgm as m_fgm ,
	   fga as m_fga ,
	   fg3m as m_fg3m ,
	   fg3a as m_fg3a ,
	   ftm as m_ftm ,
	   fta as m_fta ,
	   oreb as m_oreb ,
	   dreb as m_dreb ,
	   reb as m_reb ,
	   ast as m_ast ,
	   blk as m_blk ,
	   "TO" as m_turnovers ,
	   pf as m_pf ,
	   pts as m_pts ,
	   plus_minus as m_plus_minus 
from deduped
where row_num =1;



--- Question 2
drop table user_devices_cumulated;
CREATE TABLE user_devices_cumulated (
    user_id NUMERIC,
    browser_type TEXT,
    device_activity_datelist DATE[],  -- List of dates the user was active for this browser type
    date DATE,            -- Current date
    PRIMARY KEY (user_id, browser_type, date)
);


--- Question 3

WITH yesterday AS (
    SELECT * FROM user_devices_cumulated
    WHERE date = DATE('2022-12-31')
),
today AS (
    SELECT 
        e.user_id::NUMERIC,
        d.browser_type,
        DATE_TRUNC('day', e.event_time::TIMESTAMP) AS today_date, 
        COUNT(1) AS num_events
    FROM events e
    JOIN devices d ON e.device_id = d.device_id
    WHERE DATE_TRUNC('day', e.event_time::TIMESTAMP) = DATE('2023-01-01') 
      AND e.user_id IS NOT NULL
    GROUP BY e.user_id, d.browser_type, DATE_TRUNC('day', e.event_time::TIMESTAMP)
)
INSERT INTO user_devices_cumulated
SELECT
       COALESCE(t.user_id, y.user_id) AS user_id,
       COALESCE(t.browser_type, y.browser_type) AS browser_type,
       COALESCE(y.device_activity_datelist, ARRAY[]::DATE[])
            || CASE WHEN t.user_id IS NOT NULL
                   THEN ARRAY[t.today_date]
                   ELSE ARRAY[]::DATE[]
               END AS device_activity_datelist,
       COALESCE(t.today_date, y.date + INTERVAL '1 day') AS date
FROM yesterday y
FULL OUTER JOIN today t ON t.user_id = y.user_id AND t.browser_type = y.browser_type;


select * from user_devices_cumulated;


WITH yesterday AS (
    SELECT * FROM user_devices_cumulated
    WHERE date = DATE('2023-01-02')
),
today AS (
    SELECT 
        e.user_id::NUMERIC,
        d.browser_type,
        DATE_TRUNC('day', e.event_time::TIMESTAMP) AS today_date, 
        COUNT(1) AS num_events
    FROM events e
    JOIN devices d ON e.device_id = d.device_id
    WHERE DATE_TRUNC('day', e.event_time::TIMESTAMP) = DATE('2023-01-03') 
      AND e.user_id IS NOT NULL
    GROUP BY e.user_id, d.browser_type, DATE_TRUNC('day', e.event_time::TIMESTAMP)
)
INSERT INTO user_devices_cumulated
SELECT
       COALESCE(t.user_id, y.user_id) AS user_id,
       COALESCE(t.browser_type, y.browser_type) AS browser_type,
       COALESCE(y.device_activity_datelist, ARRAY[]::DATE[])
            || CASE WHEN t.user_id IS NOT NULL
                   THEN ARRAY[t.today_date]
                   ELSE ARRAY[]::DATE[]
               END AS device_activity_datelist,
       COALESCE(t.today_date, y.date + INTERVAL '1 day') AS date
FROM yesterday y
FULL OUTER JOIN today t ON t.user_id = y.user_id AND t.browser_type = y.browser_type;

select * from user_devices_cumulated;


--- Question 4
CREATE TABLE user_devices_datelist_int (
    user_id NUMERIC,
    browser_type TEXT,
    datelist_int BIT(32),
    date DATE,
    PRIMARY KEY (user_id, browser_type, date)
);

WITH starter AS (
    SELECT 
        ud.user_id,
        ud.browser_type,
        ud.device_activity_datelist,
        DATE_TRUNC('month', ud.date) AS month_start,
        generate_series(DATE_TRUNC('month', ud.date), DATE_TRUNC('month', ud.date) + INTERVAL '1 month' - INTERVAL '1 day', INTERVAL '1 day') AS valid_date
    FROM user_devices_cumulated ud
    WHERE ud.date = '2023-01-03'  -- Replace with the current date
),
activity_flags AS (
    SELECT 
        user_id,
        browser_type,
        month_start,
        valid_date,
        device_activity_datelist @> ARRAY[valid_date::DATE] AS is_active,  -- Cast valid_date to DATE
        EXTRACT(DAY FROM valid_date - month_start) AS days_since
    FROM starter
),
datelist_bits AS (
    SELECT 
        user_id,
        browser_type,
        month_start,
        SUM(CASE WHEN is_active THEN POW(2, 31 - days_since) ELSE 0 END)::BIGINT::BIT(32) AS datelist_int
    FROM activity_flags
    GROUP BY user_id, browser_type, month_start
)
INSERT INTO user_devices_datelist_int (user_id, browser_type, datelist_int, date)
SELECT 
    user_id,
    browser_type,
    datelist_int,
    month_start + INTERVAL '1 month' - INTERVAL '1 day'  -- End of the month
FROM datelist_bits;



SELECT * FROM user_devices_datelist_int;



--- Question 5
CREATE TABLE hosts_cumulated (
    host TEXT,
    host_activity_datelist DATE[],  -- List of dates the host experienced activity
    date DATE,                      -- Current date
    PRIMARY KEY (host, date)
);


--- Question 6
WITH yesterday AS (
    SELECT * FROM hosts_cumulated
    WHERE date = DATE('2023-01-02')
),
today AS (
    SELECT host,
           DATE_TRUNC('day', event_time::TIMESTAMP) AS today_date,
           COUNT(1) AS num_events
    FROM events
    WHERE DATE_TRUNC('day', event_time::TIMESTAMP) = DATE('2023-01-03')
      AND host IS NOT NULL
    GROUP BY host, DATE_TRUNC('day', event_time::TIMESTAMP)
)
INSERT INTO hosts_cumulated (host, host_activity_datelist, date)
SELECT
       COALESCE(t.host, y.host),
       COALESCE(y.host_activity_datelist, ARRAY[]::DATE[])
            || CASE WHEN t.host IS NOT NULL
                   THEN ARRAY[t.today_date]
                   ELSE ARRAY[]::DATE[]
               END AS host_activity_datelist,
       COALESCE(t.today_date, y.date + INTERVAL '1 day') AS date
FROM yesterday y
FULL OUTER JOIN today t ON t.host = y.host;

-- Verify the inserted data
SELECT * FROM hosts_cumulated;



--- Question 7
CREATE TABLE host_activity_reduced (
    month DATE,
    host TEXT,
    hit_array INTEGER[],          -- Array of daily hit counts
    unique_visitors_array INTEGER[],  -- Array of daily unique visitor counts
    PRIMARY KEY (month, host)
);


--- Question 8
WITH daily_data AS (
    SELECT 
        host,
        DATE_TRUNC('day', event_time::TIMESTAMP) AS day,
        COUNT(1) AS daily_hits,
        COUNT(DISTINCT user_id) AS daily_unique_visitors
    FROM events
    WHERE DATE_TRUNC('day', event_time::TIMESTAMP) = DATE('2023-01-02')  -- Replace with the current date
    GROUP BY host, DATE_TRUNC('day', event_time::TIMESTAMP)
),
monthly_data AS (
    SELECT 
        DATE_TRUNC('month', day) AS month,
        host,
        ARRAY_AGG(daily_hits ORDER BY day) AS daily_hits_array,
        ARRAY_AGG(daily_unique_visitors ORDER BY day) AS daily_unique_visitors_array
    FROM daily_data
    GROUP BY DATE_TRUNC('month', day), host
)
INSERT INTO host_activity_reduced (month, host, hit_array, unique_visitors_array)
SELECT 
    COALESCE(m.month, h.month) AS month,
    COALESCE(m.host, h.host) AS host,
    COALESCE(h.hit_array, ARRAY[]::INTEGER[]) || COALESCE(m.daily_hits_array, ARRAY[]::INTEGER[]) AS hit_array,
    COALESCE(h.unique_visitors_array, ARRAY[]::INTEGER[]) || COALESCE(m.daily_unique_visitors_array, ARRAY[]::INTEGER[]) AS unique_visitors_array
FROM monthly_data m
FULL OUTER JOIN host_activity_reduced h
ON m.month = h.month AND m.host = h.host
ON CONFLICT (month, host)
DO 
UPDATE SET 
    hit_array = EXCLUDED.hit_array,
    unique_visitors_array = EXCLUDED.unique_visitors_array;

-- Verify the inserted data
SELECT * FROM host_activity_reduced;
