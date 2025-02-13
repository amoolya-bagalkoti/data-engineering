select * from actor_films;


select filmid, count(actorid) from actor_films 
group by filmid;


select actorid, count(filmid) from actor_films 
group by actorid;


--- Lab 1
--- Cumulative table designe
select * from player_seasons;

select count(*) from player_seasons;

 CREATE TYPE season_stats AS (
                         season Integer,
                         pts REAL,
                         ast REAL,
                         reb REAL,
                         weight INTEGER
                       );
                      
 CREATE TYPE scoring_class AS
     ENUM ('bad', 'average', 'good', 'star');
    
    
 CREATE TABLE players (
     player_name TEXT,
     height TEXT,
     college TEXT,
     country TEXT,
     draft_year TEXT,
     draft_round TEXT,
     draft_number TEXT,
     seasons season_stats[],
     scoring_class scoring_class,
     years_since_last_active INTEGER,
     is_active BOOLEAN,
     current_season INTEGER,
     PRIMARY KEY (player_name, current_season)
 );

select min(season) from player_seasons;
1996
select max(season) from player_seasons;
2022

--- truncate table players;
--- pipeline query for cumulative table
WITH last_season AS (
    SELECT * FROM players
    WHERE current_season = 2021

), this_season AS (
     SELECT * FROM player_seasons
    WHERE season = 2022
)
INSERT INTO players
SELECT
        COALESCE(ls.player_name, ts.player_name) as player_name,
        COALESCE(ls.height, ts.height) as height,
        COALESCE(ls.college, ts.college) as college,
        COALESCE(ls.country, ts.country) as country,
        COALESCE(ls.draft_year, ts.draft_year) as draft_year,
        COALESCE(ls.draft_round, ts.draft_round) as draft_round,
        COALESCE(ls.draft_number, ts.draft_number)
            as draft_number,
        COALESCE(ls.seasons,
            ARRAY[]::season_stats[]
            ) || CASE WHEN ts.season IS NOT NULL THEN
                ARRAY[ROW(
                ts.season,
                ts.pts,
                ts.ast,
                ts.reb, ts.weight)::season_stats]
                ELSE ARRAY[]::season_stats[] END
            as seasons,
         CASE
             WHEN ts.season IS NOT NULL THEN
                 (CASE WHEN ts.pts > 20 THEN 'star'
                    WHEN ts.pts > 15 THEN 'good'
                    WHEN ts.pts > 10 THEN 'average'
                    ELSE 'bad' END)::scoring_class
             ELSE ls.scoring_class
         END as scoring_class,
         case when ts.season is not null then 0
              else ls.years_since_last_active +1 
         end as years_since_last_active,
         ts.season IS NOT NULL as is_active,
         COALESCE(ts.season, ls.current_season + 1) AS current_season

    FROM last_season ls
    FULL OUTER JOIN this_season ts
    ON ls.player_name = ts.player_name;
   

select * from players;

select count (distinct player_name) from players;
   
select count(*) from players;
3774
  
select * from players 
where 
current_season = 2001 and
player_name like 'Michael Jordan';

--- to unnest the season_stats struct
with unnested as (
select player_name,
unnest(seasons)::season_stats as seasons
from players
where 
current_season = 2001 and
player_name like 'Michael Jordan'
)
select player_name,
(seasons::season_stats).*
from unnested;


 SELECT player_name,
        (seasons[cardinality(seasons)]::season_stats).pts/
         CASE WHEN (seasons[1]::season_stats).pts = 0 THEN 1
             ELSE  (seasons[1]::season_stats).pts END
            AS ratio_most_recent_to_first
 FROM players
 WHERE current_season = 1998;
  	
  
--- Lab 2

select * from players;

select max(current_season) from players;

drop table players_scd_table;

create table players_scd_table
(
	player_name text,
	scoring_class scoring_class,
	is_active boolean,
	start_season integer,
	end_season integer,
	current_season INTEGER,
	primary key(player_name, start_season, end_season, current_season)
);

--- scd table generator query

select * from players_scd_table;

truncate table players_scd_table;

insert INTO players_scd_table 
WITH streak_started AS (
    SELECT player_name,
           current_season,
           is_active,
           scoring_class,
           LAG(scoring_class, 1) OVER
               (PARTITION BY player_name ORDER BY current_season) <> scoring_class
               OR LAG(scoring_class, 1) OVER
               (PARTITION BY player_name ORDER BY current_season) IS NULL
               AS did_change
    FROM players
    where current_season <= 2001
),
     streak_identified AS (
         SELECT
            player_name,
                scoring_class,
                current_season,
                is_active,
            SUM(CASE WHEN did_change THEN 1 ELSE 0 END)
                OVER (PARTITION BY player_name ORDER BY current_season) as streak_identifier
         FROM streak_started
     ),
     aggregated AS (
         SELECT
            player_name,
            scoring_class,
            streak_identifier,
            is_active,
            MIN(current_season) AS start_season,
            MAX(current_season) AS end_season,
            2001 as current_season
         FROM streak_identified
         GROUP BY 1,2,3,4
     )

     SELECT player_name, scoring_class, is_active, start_season, end_season, current_season
     FROM aggregated;

--- the above query scans the entire history in every run. it is not incremental. 2001, 2002 so on future loads  all the history is recalculated.
--- prone to out of memory exception.

    
    
CREATE TYPE scd_type AS (
                    scoring_class scoring_class,
                    is_active boolean,
                    start_season INTEGER,
                    end_season INTEGER
                        )
                        
--- query for incremental load
   
WITH last_season_scd AS (
    SELECT * FROM players_scd_table          --- players_scd_table is the previous load 
    WHERE current_season = 2021
    AND end_season = 2021
),
     historical_scd AS (
        SELECT
            player_name,
               scoring_class,
               is_active,
               start_season,
               end_season
        FROM players_scd_table
        WHERE current_season = 2021
        AND end_season < 2021
     ),
     this_season_data AS (
         SELECT * FROM players
         WHERE current_season = 2022
     ),
     unchanged_records AS (
         SELECT
                ts.player_name,
                ts.scoring_class,
                ts.is_active,
                ls.start_season,
                ts.current_season as end_season
        FROM this_season_data ts
        JOIN last_season_scd ls
        ON ls.player_name = ts.player_name
         WHERE ts.scoring_class = ls.scoring_class
         AND ts.is_active = ls.is_active
     ),
     changed_records AS (
        SELECT
                ts.player_name,
                UNNEST(ARRAY[
                    ROW(
                        ls.scoring_class,
                        ls.is_active,
                        ls.start_season,
                        ls.end_season

                        )::scd_type,
                    ROW(
                        ts.scoring_class,
                        ts.is_active,
                        ts.current_season,
                        ts.current_season
                        )::scd_type
                ]) as records
        FROM this_season_data ts
        LEFT JOIN last_season_scd ls
        ON ls.player_name = ts.player_name
         WHERE (ts.scoring_class <> ls.scoring_class
          OR ts.is_active <> ls.is_active)
     ),
     unnested_changed_records AS (

         SELECT player_name,
                (records::scd_type).scoring_class,
                (records::scd_type).is_active,
                (records::scd_type).start_season,
                (records::scd_type).end_season
                FROM changed_records
         ),
     new_records AS (

         SELECT
            ts.player_name,
                ts.scoring_class,
                ts.is_active,
                ts.current_season AS start_season,
                ts.current_season AS end_season
         FROM this_season_data ts
         LEFT JOIN last_season_scd ls
             ON ts.player_name = ls.player_name
         WHERE ls.player_name IS NULL

     )


SELECT *, 2022 AS current_season FROM (
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
              ) a

--- Lab 3
--- Graph Data Modeling

CREATE TYPE vertex_type
    AS ENUM('player', 'team', 'game');



CREATE TABLE vertices (
    identifier TEXT,
    type vertex_type,
    properties JSON,
    PRIMARY KEY (identifier, type)
);

CREATE TYPE edge_type AS
    ENUM ('plays_against',
          'shares_team',
          'plays_in',
          'plays_on'
        );

CREATE TABLE edges (
    subject_identifier TEXT,
    subject_type vertex_type,
    object_identifier TEXT,
    object_type vertex_type,
    edge_type edge_type,
    properties JSON,
    PRIMARY KEY (subject_identifier,
                subject_type,
                object_identifier,
                object_type,
                edge_type)
);

select * from games;

select * from game_details;


select * from teams;

--- vertices query to load vertices table
insert into vertices
WITH teams_deduped AS (
    SELECT *, ROW_NUMBER() OVER(PARTITION BY team_id) as row_num
    FROM teams
)
SELECT
       team_id AS identifier,
    'team'::vertex_type AS type,
    json_build_object(
        'abbreviation', abbreviation,
        'nickname', nickname,
        'city', city,
        'arena', arena,
        'year_founded', yearfounded
        )
FROM teams_deduped
WHERE row_num = 1

select * from vertices;
              
 --- player - games edges loading query             
INSERT INTO edges
WITH deduped AS (
    SELECT *, row_number() over (PARTITION BY player_id, game_id) AS row_num
    FROM game_details
)
SELECT
    player_id AS subject_identifier,
    'player'::vertex_type as subject_type,
    game_id AS object_identifier,
    'game'::vertex_type AS object_type,
    'plays_in'::edge_type AS edge_type,
    json_build_object(
        'start_position', start_position,
        'pts', pts,
        'team_id', team_id,
        'team_abbreviation', team_abbreviation
        ) as properties
FROM deduped
WHERE row_num = 1;              
              
select * from edges;             

--- player - player leading query
INSERT INTO edges 
 WITH deduped AS (
    SELECT *, row_number() over (PARTITION BY player_id, game_id) AS row_num
    FROM game_details
),
     filtered AS (
         SELECT * FROM deduped
         WHERE row_num = 1
     ),
     aggregated AS (
          SELECT
           f1.player_id as subject_player_id,           
           f2.player_id as object_player_id,           
           CASE WHEN f1.team_abbreviation =         f2.team_abbreviation
                THEN 'shares_team'::edge_type
            ELSE 'plays_against'::edge_type
            end as edge_type,
            MAX(f1.player_name) subject_player_name,
            MAX(f2.player_name) as object_player_name,
            COUNT(1) AS num_games,
            SUM(f1.pts) AS subject_points,
            SUM(f2.pts) as object_points
        FROM filtered f1
            JOIN filtered f2
            ON f1.game_id = f2.game_id
            AND f1.player_name <> f2.player_name
        WHERE f1.player_id > f2.player_id      --- to avoid duplicate edges. else we would have a->b and b->a which is ideally same
        GROUP BY
            f1.player_id,
           f2.player_id,
           CASE WHEN f1.team_abbreviation =         f2.team_abbreviation
                THEN  'shares_team'::edge_type
            ELSE 'plays_against'::edge_type
            end)
            
       select subject_player_id as subject_identifier,
       'player'::vertex_type as subject_type,
       object_player_id as object_identifier,
       'player'::vertex_type as object_type,
       edge_type as edge_type,
       json_build_object(
       'num_games', num_games,
       'subject_points', subject_points,
       'object_points', object_points
       ) as properties
      from aggregated;
     
     
     
 select distinct edge_type from edges;             
              
 select * from edges;             
              
              
 --- load player and game data into vertices similar to how teams is done.             
--- try below query after all vertices data is ready.
              
 select v.properties->>'player_name', e.object_identifer,
 cast(v.properties->>'number_of_games' as real)/
 case when cast(v.properties->>'total_points' as real) = 0 then 1 else cast(v.properties->>'total_points' as real) end as points_per_game,
 e.properties->>'subject_points',
 e.properties->>'num_games'
 from vertices v join edges e
 on v.identifier = e.subject_identifier
 and v.type = e.subject_type
 where e.object_type = 'player'::vertex_type
              
              
              
