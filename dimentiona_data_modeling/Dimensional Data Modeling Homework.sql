select * from actor_films;

--- Question 1
DROP TYPE films;
CREATE TYPE films AS (
	release_year INT,
    film VARCHAR(255),        
    votes INT,                
    rating FLOAT,             
    filmid TEXT               
);

CREATE TYPE quality_class AS ENUM (
    'star',   
    'good',   
    'average', 
    'bad'     
);

drop table actors;

CREATE TABLE actors (
	actor_id TEXT,
	actor_name TEXT,
	films films[],
	quality_class quality_class,
	is_active boolean,
	years_since_last_active INTEGER ,
	current_year INTEGER
	PRIMARY KEY (actor_id, actor_name, current_year)
);


--- Question 2
select * from actor_films;

select min(year), max(year) from actor_films;
1970, 2021

INSERT INTO actors (actor_id, actor_name, films, quality_class, is_active, years_since_last_active, current_year)
VALUES ('tt0064779', 'Actor Name', ARRAY[]::films[], 'star', true, 5, 2025);

select * from actors;

truncate table actors;

--- cumulative table
WITH last_year AS (
    SELECT * FROM actors
    WHERE current_year = 1973
),
this_year AS (
    SELECT * FROM actor_films
    WHERE year = 1974
),
processed AS (
    SELECT
        COALESCE(ly.actor_id, ty.actorid) AS actor_id,
        COALESCE(ly.actor_name, ty.actor) AS actor_name,
        COALESCE(ly.films, ARRAY[]::films[]) || 
            CASE 
                WHEN ty.film IS NOT NULL THEN 
                    ARRAY[ROW(ty.year, ty.film, ty.votes, ty.rating, ty.filmid)::films]
                ELSE ARRAY[]::films[] 
            END AS films,
        CASE 
            WHEN ty.film IS NOT NULL THEN 
                CASE 
                    WHEN ty.rating > 8 THEN 'star'
                    WHEN ty.rating > 7 THEN 'good'
                    WHEN ty.rating > 6 THEN 'average'
                    ELSE 'bad' 
                END::quality_class
            ELSE ly.quality_class
        END AS quality_class,
        ty.year IS NOT NULL AS is_active,
        CASE 
            WHEN ty.film IS NOT NULL THEN 0
            ELSE ly.years_since_last_active + 1
        END AS years_since_last_active,
        COALESCE(ty.year, ly.current_year + 1) AS current_year
    FROM last_year ly
    FULL OUTER JOIN this_year ty
        ON ly.actor_id = ty.actorid AND ly.actor_name = ty.actor
)
insert into actors
SELECT 
    actor_id,
    actor_name,
    ARRAY_AGG(f) AS films,
    MAX(quality_class) AS quality_class,
    MAX(CASE WHEN is_active THEN 1 ELSE 0 END)::boolean AS is_active,
    MAX(years_since_last_active) AS years_since_last_active,
    MAX(current_year) AS current_year
FROM (
    SELECT 
        actor_id,
        actor_name,
        UNNEST(films) AS f,
        quality_class,
        is_active,
        years_since_last_active,
        current_year
    FROM processed
) sub
GROUP BY actor_id, actor_name;
   
   
select * from actor_films;  
select * from actors;   



--- Question 3
drop table actors_history_scd;
CREATE TABLE actors_history_scd (
    actor_id TEXT,         
    actor_name TEXT,       
    quality_class quality_class,    
    is_active BOOLEAN,             
    start_year INT,       
    end_year INT,                  
    current_year INT, 
    primary KEY (actor_id, start_year, end_year, current_year) 
);



--- Question 4
--- backfill query
INSERT INTO actors_history_scd
WITH changes_detected AS (
    SELECT actor_id,
        actor_name,
        current_year,
        quality_class,
        is_active,
        LAG(quality_class) OVER (PARTITION BY actor_name ORDER BY current_year) <> quality_class
            OR LAG(is_active) OVER (PARTITION BY actor_name ORDER BY current_year) <> is_active
            OR LAG(quality_class) OVER (PARTITION BY actor_name ORDER BY current_year) IS NULL
            OR LAG(is_active) OVER (PARTITION BY actor_name ORDER BY current_year) IS NULL AS has_changed
    FROM actors
    where current_year <=1972
),
streak_identified AS (
    SELECT actor_id,
        actor_name,
        quality_class,
        is_active,
        current_year,
        SUM(CASE WHEN has_changed THEN 1 ELSE 0 END) 
            OVER (PARTITION BY actor_name ORDER BY current_year) AS streak_identifier
    FROM changes_detected
),
aggregated AS (
    SELECT actor_id,
        actor_name,
        quality_class,
        is_active,
        MIN(current_year) AS start_year,
        MAX(current_year) AS end_year,
        1972 AS current_year
    FROM streak_identified
    GROUP BY actor_id, actor_name, quality_class, is_active, streak_identifier
)
SELECT actor_id,
    actor_name,
    quality_class,
    is_active,
    start_year,
    end_year,
    current_year
FROM aggregated;


select * from actors_history_scd;

truncate table actors_history_scd;





--- Question 5
--- scd incremental load query

/*
CREATE TYPE act_scd_type AS (
    quality_class quality_class, 
    is_active BOOLEAN,            
    start_year INTEGER,           
    end_year INTEGER             
);
*/

-- Define the SCD type for handling changes
DROP TYPE IF EXISTS scd_type;
CREATE TYPE scd_type AS (
    quality_class quality_class,
    is_active BOOLEAN,
    start_year INT,
    end_year INT
);

-- Incremental query for actors history SCD
WITH last_year_scd AS (
    SELECT * FROM actors_history_scd
    WHERE current_year = 1972
    AND end_year = 1972
),
     historical_scd AS (
        SELECT
            actor_id,
            actor_name,
            quality_class,
            is_active,
            start_year,
            end_year
        FROM actors_history_scd
        WHERE current_year = 1972
        AND end_year < 1972
     ),
     this_year_data AS (
         SELECT * FROM actors
         WHERE current_year = 1973
     ),
     unchanged_records AS (
         SELECT
                ts.actor_id,
                ts.actor_name,
                ts.quality_class,
                ts.is_active,
                ls.start_year,
                ts.current_year as end_year
        FROM this_year_data ts
        JOIN last_year_scd ls
        ON ls.actor_id = ts.actor_id
         WHERE ts.quality_class = ls.quality_class
         AND ts.is_active = ls.is_active
     ),
     changed_records AS (
        SELECT
                ts.actor_id,
                ts.actor_name,
                UNNEST(ARRAY[
                    ROW(
                        ls.quality_class,
                        ls.is_active,
                        ls.start_year,
                        ls.end_year
                        )::scd_type,
                    ROW(
                        ts.quality_class,
                        ts.is_active,
                        ts.current_year,
                        ts.current_year
                        )::scd_type
                ]) as records
        FROM this_year_data ts
        LEFT JOIN last_year_scd ls
        ON ls.actor_id = ts.actor_id
         WHERE (ts.quality_class <> ls.quality_class
          OR ts.is_active <> ls.is_active)
     ),
     unnested_changed_records AS (
         SELECT actor_id,
                actor_name,
                (records::scd_type).quality_class,
                (records::scd_type).is_active,
                (records::scd_type).start_year,
                (records::scd_type).end_year
         FROM changed_records
     ),
     new_records AS (
         SELECT
            ts.actor_id,
            ts.actor_name,
            ts.quality_class,
            ts.is_active,
            ts.current_year AS start_year,
            ts.current_year AS end_year
         FROM this_year_data ts
         LEFT JOIN last_year_scd ls
             ON ts.actor_id = ls.actor_id
         WHERE ls.actor_id IS NULL
     )

-- Combine and insert records into actors_scd table
INSERT INTO actors_history_scd (actor_id, actor_name, quality_class, is_active, start_year, end_year, current_year)

SELECT *, 1973 AS current_year FROM (
                  SELECT *
                  FROM historical_scd

                  UNION ALL

                  SELECT *
                  FROM unchanged_records

                  UNION ALL

                  SELECT *
                  FROM unnested_changed_records

                  UNION ALL

                  SELECT *
                  FROM new_records
              ) a;




select * from actors_history_scd;
