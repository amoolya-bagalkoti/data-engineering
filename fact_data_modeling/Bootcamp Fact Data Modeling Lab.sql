select * from game_details;


select * from games;


--- Lab 1
--- Fact data modeling
create table fact_game_details(
dim_game_date DATE,
dim_season INTEGER,
dim_team_id INTEGER,
dim_player_id INTEGER,
dim_player_name text,
dim_start_position text,
dim_is_playing_at_home BOOLEAN,
dim_did_not_play BOOLEAN,
dim_did_not_dress BOOLEAN,
dim_not_with_team BOOLEAN,
m_minutes real,
m_fgm INT,
m_fga INT,
m_fg3m INT,
m_fg3a INT,
m_ftm INT,
m_fta INT,
m_oreb INT,
m_dreb INT,
m_reb INT,
m_ast INT,
m_blk INT,
m_turnovers INT,
m_pf INT,
m_pts INT,
m_plus_minus INT,
primary key (dim_game_date, dim_team_id, dim_player_id)
);




insert into fact_game_details 
with deduped as (
select g.game_date_est,
	   g.season,
	   g.home_team_id,
	   gd.*,
	   row_number() over (partition by gd.game_id, team_id, player_id) as row_num
from game_details gd 
join games g 
on gd.game_id = g.game_id
/*where g.game_date_est = '2016-10-04'*/
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



select * from fact_game_details ;



select t.*, gd.*
from fact_game_details gd
join teams t 
on t.team_id = gd.dim_team_id;


--- Lab 2

select * from events;

drop table users_cumulated;

 CREATE TABLE users_cumulated (
     user_id TEXT,
     dates_active DATE[], --- list of dates in past where user was active
     date DATE,           --- current date
     PRIMARY KEY (user_id, date)
 );
 



WITH yesterday AS (
    SELECT * FROM users_cumulated
    WHERE date = DATE('2022-12-31')
),
today AS (
    SELECT user_id::TEXT,
           DATE_TRUNC('day', event_time::TIMESTAMP) AS today_date, 
           COUNT(1) AS num_events
    FROM events
    WHERE DATE_TRUNC('day', event_time::TIMESTAMP) = DATE('2023-01-01') 
      AND user_id IS NOT NULL
    GROUP BY user_id, DATE_TRUNC('day', event_time::TIMESTAMP)
)
INSERT INTO users_cumulated
SELECT
       COALESCE(t.user_id, y.user_id),
       COALESCE(y.dates_active, ARRAY[]::DATE[])
            || CASE WHEN t.user_id IS NOT NULL
                   THEN ARRAY[t.today_date]
                   ELSE ARRAY[]::DATE[]
               END AS date_list,
       COALESCE(t.today_date, y.date + INTERVAL '1 day') AS date
FROM yesterday y
FULL OUTER JOIN today t ON t.user_id = y.user_id;


select * from users_cumulated;


WITH yesterday AS (
    SELECT * FROM users_cumulated
    WHERE date = DATE('2023-01-04')
),
today AS (
    SELECT user_id::TEXT,
           DATE_TRUNC('day', event_time::TIMESTAMP) AS today_date, 
           COUNT(1) AS num_events
    FROM events
    WHERE DATE_TRUNC('day', event_time::TIMESTAMP) = DATE('2023-01-05') 
      AND user_id IS NOT NULL
    GROUP BY user_id, DATE_TRUNC('day', event_time::TIMESTAMP)
)
INSERT INTO users_cumulated
SELECT
       COALESCE(t.user_id, y.user_id),
       COALESCE(y.dates_active, ARRAY[]::DATE[])
            || CASE WHEN t.user_id IS NOT NULL
                   THEN ARRAY[t.today_date]
                   ELSE ARRAY[]::DATE[]
               END AS date_list,
       COALESCE(t.today_date, y.date + INTERVAL '1 day') AS date
FROM yesterday y
FULL OUTER JOIN today t ON t.user_id = y.user_id;

select * from users_cumulated;

--- above query can be run incrementally to see the list of dates users were active





--- this query can hold data for the entire month. from 2023-01-01 to 2023-01-31. used 2023-01-05 because only loaded untill 5th from previous query.
WITH starter AS (
    SELECT uc.dates_active @> ARRAY [DATE(d.valid_date)]   AS is_active,
           EXTRACT(
               DAY FROM DATE('2023-01-05') - d.valid_date) AS days_since,
           uc.user_id
    FROM users_cumulated uc
             CROSS JOIN
         (SELECT generate_series('2023-01-01', '2023-01-05', INTERVAL '1 day') AS valid_date) as d
    WHERE date = DATE('2023-01-05')
),
     bits AS (
         SELECT user_id,
                SUM(CASE
                        WHEN is_active THEN POW(2, 32 - days_since)
                        ELSE 0 END)::bigint::bit(32) AS datelist_int,
                DATE('2023-03-31') as date
         FROM starter
         GROUP BY user_id
     )

/*
     INSERT INTO user_datelist_int
*/
     SELECT * FROM bits;

     

CREATE TABLE user_datelist_int (
    user_id BIGINT,
    datelist_int BIT(32),
    date DATE,
    PRIMARY KEY (user_id, date)
)
     
--- use below query to load entire month data once users_cumulated is completely loaded
WITH starter AS (
    SELECT uc.dates_active @> ARRAY [DATE(d.valid_date)]   AS is_active,
           EXTRACT(
               DAY FROM DATE('2023-01-31') - d.valid_date) AS days_since,
           uc.user_id
    FROM users_cumulated uc
             CROSS JOIN
         (SELECT generate_series('2023-01-01', '2023-01-31', INTERVAL '1 day') AS valid_date) as d
    WHERE date = DATE('2023-01-31')
),
     bits AS (
         SELECT user_id,
                SUM(CASE
                        WHEN is_active THEN POW(2, 32 - days_since)
                        ELSE 0 END)::bigint::bit(32) AS datelist_int,
                DATE('2023-03-31') as date
         FROM starter
         GROUP BY user_id
     )

     INSERT INTO user_datelist_int
     SELECT * FROM bits;
     
    
 
    
CREATE TABLE monthly_user_site_hits
(
    user_id          BIGINT,
    hit_array        BIGINT[],
    month_start      DATE,
    first_found_date DATE,
    date_partition DATE,
    PRIMARY KEY (user_id, date_partition, month_start)
);


    WITH yesterday AS (
    SELECT *
    FROM monthly_user_site_hits
    WHERE date_partition = '2023-03-02'
),
     today AS (
         SELECT user_id,
                DATE_TRUNC('day', event_time) AS today_date,
                COUNT(1) as num_hits
         FROM events
         WHERE DATE_TRUNC('day', event_time) = DATE('2023-03-03')
         AND user_id IS NOT NULL
         GROUP BY user_id, DATE_TRUNC('day', event_time)
     )
INSERT INTO monthly_user_site_hits
SELECT
    COALESCE(y.user_id, t.user_id) AS user_id,
       COALESCE(y.hit_array,
           array_fill(NULL::BIGINT, ARRAY[DATE('2023-03-03') - DATE('2023-03-01')]))
        || ARRAY[t.num_hits] AS hits_array,
    DATE('2023-03-01') as month_start,
    CASE WHEN y.first_found_date < t.today_date
        THEN y.first_found_date
        ELSE t.today_date
            END as first_found_date,
    DATE('2023-03-03') AS date_partition
    FROM yesterday y
    FULL OUTER JOIN today t
        ON y.user_id = t.user_id
        
        
 SELECT
       month_start,
       SUM(hit_array[1]) as num_hits_mar_1,
       SUM(hit_array[2]) AS num_hits_mar_2
FROM monthly_user_site_hits
WHERE date_partition = DATE('2023-03-03')
GROUP BY 1

        
--- date list analysis     
WITH starter AS (
    SELECT uc.dates_active @> ARRAY [DATE(d.valid_date)]   AS is_active,
           EXTRACT(
               DAY FROM DATE('2023-03-31') - d.valid_date) AS days_since,
           uc.user_id
    FROM users_cumulated uc
             CROSS JOIN
         (SELECT generate_series('2023-02-28', '2023-03-31', INTERVAL '1 day') AS valid_date) as d
    WHERE date = DATE('2023-03-31')
),
     bits AS (
         SELECT user_id,
                SUM(CASE
                        WHEN is_active THEN POW(2, 32 - days_since)
                        ELSE 0 END)::bigint::bit(32) AS datelist_int
         FROM starter
         GROUP BY user_id
     )

SELECT
       user_id,
       datelist_int,
       BIT_COUNT(datelist_int) > 0 AS monthly_active,
       BIT_COUNT(datelist_int) AS l32,
       BIT_COUNT(datelist_int &
       CAST('11111110000000000000000000000000' AS BIT(32))) > 0 AS weekly_active,
       BIT_COUNT(datelist_int &
       CAST('11111110000000000000000000000000' AS BIT(32)))  AS l7,

       BIT_COUNT(datelist_int &
       CAST('00000001111111000000000000000000' AS BIT(32))) > 0 AS weekly_active_previous_week
FROM bits;




--- Lab 3
        
CREATE TABLE array_metrics (
    user_id NUMERIC,
    month_start DATE,
    metric_name TEXT,
    metric_array REAL[],
PRIMARY KEY (user_id, month_start, metric_name));




/*
-- Insert aggregated metrics into the array_metrics table
INSERT INTO array_metrics
WITH daily_aggregate AS (
    -- Aggregate daily site hits per user
    SELECT 
        user_id,
        DATE(event_time) AS date,
        COUNT(1) AS num_site_hits
    FROM events
    WHERE DATE(event_time) = DATE('2023-01-01')
    AND user_id IS NOT NULL
    GROUP BY user_id, DATE(event_time)
),
yesterday_array AS (
    -- Retrieve existing metrics for the month starting from '2023-01-01'
    SELECT *
    FROM array_metrics 
    WHERE month_start = DATE('2023-01-01')
)
SELECT 
    -- Select user_id from either daily_aggregate or yesterday_array
    COALESCE( da.user_id, ya.user_id) AS user_id,
    -- Determine month_start date
    COALESCE(ya.month_start, DATE_TRUNC('month', da.date)) AS month_start,
    -- Set metric name to 'site_hits'
    'site_hits' AS metric_name,
    -- Update metric_array based on existing data and new daily aggregates
    CASE 
        WHEN ya.metric_array IS NOT NULL THEN 
            ya.metric_array || ARRAY[COALESCE(da.num_site_hits,0)] 
        WHEN ya.metric_array IS NULL THEN
            ARRAY_FILL(0, ARRAY[COALESCE (date - DATE(DATE_TRUNC('month', date)), 0)]) 
                || ARRAY[COALESCE(da.num_site_hits,0)]
    END AS metric_array
FROM daily_aggregate da
FULL OUTER JOIN yesterday_array ya 
ON da.user_id = ya.user_id
ON CONFLICT (user_id, month_start, metric_name)
DO 
    UPDATE SET metric_array = EXCLUDED.metric_array;

-- Uncomment and run the following query to verify the cardinality of metric_array
-- SELECT cardinality(metric_array), COUNT(1)
-- FROM array_metrics
-- GROUP BY 1;

-- Aggregate metrics by summing specific elements in the metric_array
WITH agg AS (
    SELECT metric_name, month_start, ARRAY[SUM(metric_array[1]), SUM(metric_array[2]), SUM(metric_array[3])] AS summed_array
    FROM array_metrics
    GROUP BY metric_name, month_start
)
-- Select and display the metric_name, date (adjusted by index), and summed value
SELECT 
    metric_name, 
    month_start + CAST(CAST(index - 1 AS TEXT) || ' day' AS INTERVAL) AS adjusted_date,
    elem AS value
FROM agg
CROSS JOIN UNNEST(agg.summed_array) WITH ORDINALITY AS a(elem, index);
*/


--- fix the above query

--- this is not the complete query
INSERT INTO array_metrics (user_id, month_start, metric_name, metric_array)
WITH daily_aggregate AS (
    -- Aggregate daily site hits per user
    SELECT 
        user_id,
        DATE(event_time) AS date,
        COUNT(1) AS num_site_hits
    FROM events
    WHERE DATE(event_time) = DATE('2023-01-01')
      AND user_id IS NOT NULL
    GROUP BY user_id, DATE(event_time)
),
yesterday_array AS (
    -- Retrieve existing metrics for the month starting from '2023-01-01'
    SELECT *
    FROM array_metrics 
    WHERE month_start = DATE('2023-01-01')
)
SELECT 
    COALESCE(da.user_id, ya.user_id) AS user_id,
    COALESCE(ya.month_start, DATE_TRUNC('month', da.date)) AS month_start,
    'site_hits' AS metric_name,
    CASE 
        WHEN ya.metric_array IS NOT NULL THEN 
            ya.metric_array || ARRAY[COALESCE(da.num_site_hits, 0)] 
        ELSE 
            ARRAY_FILL(0, ARRAY[COALESCE(da.date - DATE(DATE_TRUNC('month', da.date)), 0)]) 
                || ARRAY[COALESCE(da.num_site_hits, 0)]
    END AS metric_array
FROM daily_aggregate da
FULL OUTER JOIN yesterday_array ya 
ON da.user_id = ya.user_id



select * from array_metrics;





