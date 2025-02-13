 --- Lab 1
 CREATE TABLE users_growth_accounting (
     user_id TEXT,
     first_active_date DATE,
     last_active_date DATE,
     daily_active_state TEXT,
     weekly_active_state TEXT,
     dates_active DATE[],
     date DATE,
     PRIMARY KEY (user_id, date)
 );
 

select * from events;

select min(event_time), max(event_time) from events;
2023-01-01 2023-01-31

--- growth accounting query. similar to cumulative
insert into users_growth_accounting
WITH yesterday AS (
    SELECT * FROM users_growth_accounting
    WHERE date = DATE('2023-01-08')
),
     today AS (
         SELECT
            CAST(user_id AS TEXT) as user_id,
            DATE_TRUNC('day', event_time::timestamp) as today_date,
            COUNT(1)
         FROM events
         WHERE DATE_TRUNC('day', event_time::timestamp) = DATE('2023-01-09')
         AND user_id IS NOT NULL
         GROUP BY user_id, DATE_TRUNC('day', event_time::timestamp)
     )

         SELECT COALESCE(t.user_id, y.user_id)                    as user_id,
                COALESCE(y.first_active_date, t.today_date)       AS first_active_date,
                COALESCE(t.today_date, y.last_active_date)        AS last_active_date,
                CASE
                    WHEN y.user_id IS NULL THEN 'New'
                    WHEN y.last_active_date = t.today_date - Interval '1 day' THEN 'Retained'
                    WHEN y.last_active_date < t.today_date - Interval '1 day' THEN 'Resurrected'
                    WHEN t.today_date IS NULL AND y.last_active_date = y.date THEN 'Churned'
                    ELSE 'Stale'
                    END                                           as daily_active_state,
                CASE
                    WHEN y.user_id IS NULL THEN 'New'
                    WHEN y.last_active_date < t.today_date - Interval '7 day' THEN 'Resurrected'
                    WHEN
                            t.today_date IS NULL
                            AND y.last_active_date = y.date - interval '7 day' THEN 'Churned'
                    WHEN COALESCE(t.today_date, y.last_active_date) + INTERVAL '7 day' >= y.date THEN 'Retained'
                    ELSE 'Stale'
                    END                                           as weekly_active_state,
                COALESCE(y.dates_active,
                         ARRAY []::DATE[])
                    || CASE
                           WHEN
                               t.user_id IS NOT NULL
                               THEN ARRAY [t.today_date]
                           ELSE ARRAY []::DATE[]
                    END                                           AS date_list,
                COALESCE(t.today_date, y.date + Interval '1 day') as date
         FROM today t
                  FULL OUTER JOIN yesterday y
                                  ON t.user_id = y.user_id;
                                  
                                 
                                 
 select * from users_growth_accounting;    


--- retention analysis query
SELECT
       date - first_active_date AS days_since_first_active,
       CAST(COUNT(CASE
           WHEN daily_active_state
                    IN ('Retained', 'Resurrected', 'New') THEN 1 END) AS REAL)/COUNT(1) as pct_active,
       COUNT(1) FROM users_growth_accounting
GROUP BY date - first_active_date;



--- Lab 2

--- Funnel analysis query
--- query to see how many users who hit the sign up page and actually signed up

select * from events
where url = '/api/v1/user';


WITH deduped_events AS (
    SELECT
        url, host, user_id,event_time
    FROM events
    GROUP BY 1,2,3,4
),
     clean_events AS (
         SELECT *, DATE(event_time) as event_date FROM deduped_events
WHERE user_id IS NOT NULL
ORDER BY user_id, event_time
     ),
     converted AS (
         SELECT ce1.user_id,
                ce1.event_time,
                ce1.url,
                COUNT(DISTINCT CASE WHEN ce2.url = '/api/v1/user' THEN ce2.url END) as converted
         FROM clean_events ce1
                  JOIN clean_events ce2
                       ON ce2.user_id = ce1.user_id
                           AND ce2.event_date = ce1.event_date
                           AND ce2.event_time > ce1.event_time

         GROUP BY 1, 2,3
     )

SELECT url, COUNT(1), CAST(SUM(converted) AS REAL)/COUNT(1)
FROM converted
GROUP BY 1
HAVING CAST(SUM(converted) AS REAL)/COUNT(1) > 0
AND COUNT(1) > 100


--- Grouping sets
CREATE TABLE device_hits_dashboard AS
WITH events_augmented AS (
    SELECT COALESCE(d.os_type, 'unknown')      AS os_type,
           COALESCE(d.device_type, 'unknown')  AS device_type,
           COALESCE(d.browser_type, 'unknown') AS browser_type,
           url,
           user_id
    FROM events e
             JOIN devices d on e.device_id = d.device_id
)

SELECT
       CASE
           WHEN GROUPING(os_type) = 0
               AND GROUPING(device_type) = 0
               AND GROUPING(browser_type) = 0
               THEN 'os_type__device_type__browser'
           WHEN GROUPING(browser_type) = 0 THEN 'browser_type'
           WHEN GROUPING(device_type) = 0 THEN 'device_type'
           WHEN GROUPING(os_type) = 0 THEN 'os_type'
       END as aggregation_level,
       COALESCE(os_type, '(overall)') as os_type,
       COALESCE(device_type, '(overall)') as device_type,
       COALESCE(browser_type, '(overall)') as browser_type,
       COUNT(1) as number_of_hits
FROM events_augmented
GROUP BY GROUPING SETS (
        (browser_type, device_type, os_type),
        (browser_type),
        (os_type),
        (device_type)
    )
ORDER BY COUNT(1) desc;


select * from device_hits_dashboard;





---window based analysis query
WITH events_augmented AS (
    SELECT COALESCE(d.os_type, 'unknown')      AS os_type,
           COALESCE(d.device_type, 'unknown')  AS device_type,
           COALESCE(d.browser_type, 'unknown') AS browser_type,
           url,
           user_id,
           CASE
               WHEN referrer like '%linkedin%' THEN 'Linkedin'
               WHEN referrer like '%t.co%' THEN 'Twitter'
               WHEN referrer like '%google%' THEN 'Google'
               WHEN referrer like '%lnkd%' THEN 'Linkedin'
               WHEN referrer like '%eczachly%' THEN 'On Site'
               WHEN referrer LIKE '%zachwilson%' THEN 'On Site'
               ELSE referrer
               END                             as referrer,
           DATE(event_time)                    AS event_date
    FROM events e
             JOIN devices d on e.device_id = d.device_id
),
     aggregated AS (
         SELECT url, referrer, event_date, COUNT(1) as count
         FROM events_augmented
         GROUP BY url, referrer, event_date
     ),
     windowed AS (
         SELECT referrer,
                url,
                event_date,
                count,
                SUM(count) OVER (
                    PARTITION BY referrer, url, DATE_TRUNC('month', event_date)
                    ORDER BY event_date
                    ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
                    ) AS monthly_cumulative_sum,
                SUM(count) OVER (
                    PARTITION BY referrer, url
                    ORDER BY event_date
                    ) AS rolling_cumulative_sum,
                SUM(count) OVER (
                    PARTITION BY referrer, url
                    ORDER BY event_date
                    ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
                    ) AS total_cumulative_sum,
                SUM(count) OVER (
                    PARTITION BY referrer, url
                    ORDER BY event_date
                    ROWS BETWEEN 6 preceding AND CURRENT ROW
                    ) AS weekly_rolling_count,
                SUM(count) OVER (
                    PARTITION BY referrer, url
                    ORDER BY event_date
                    ROWS BETWEEN 13 preceding AND 6 preceding
                    ) AS previous_weekly_rolling_count
         FROM aggregated
         ORDER BY referrer, url, event_date
     )

SELECT referrer,
       url,
       event_date,
       count,
       weekly_rolling_count,
       previous_weekly_rolling_count,
       CAST(count AS REAL) / monthly_cumulative_sum as pct_of_month,
       CAST(count AS REAL) / total_cumulative_sum   as pct_of_total
FROM windowed
WHERE total_cumulative_sum > 500
  AND referrer IS NOT null;

                                 