--- Question 1
--- state change tracking for players
WITH previous_season AS (
    SELECT * FROM players
    WHERE current_season = (SELECT MAX(current_season) FROM players) - 1
),
current_season AS (
    SELECT * FROM players
    WHERE current_season = (SELECT MAX(current_season) FROM players)
),
state_changes AS (
    SELECT
        COALESCE(cs.player_name, ps.player_name) AS player_name,
        CASE
            WHEN ps.player_name IS NULL THEN 'New'
            WHEN cs.player_name IS NULL THEN 'Retired'
            WHEN ps.is_active = FALSE AND cs.is_active = TRUE THEN 'Returned from Retirement'
            WHEN ps.is_active = TRUE AND cs.is_active = TRUE THEN 'Continued Playing'
            WHEN ps.is_active = FALSE AND cs.is_active = FALSE THEN 'Stayed Retired'
        END AS state
    FROM previous_season ps
    FULL OUTER JOIN current_season cs ON ps.player_name = cs.player_name
)
SELECT * FROM state_changes
ORDER BY player_name;


--- Question 2
--- Efficient Aggregations with GROUPING SETS
create table aggregated_game_details as
WITH aggregated AS (
    SELECT
        player_name,
        team_abbreviation,
        SUM(pts) AS total_points,
        COUNT(DISTINCT game_id) AS games_played,
        COUNT(CASE WHEN plus_minus > 0 THEN 1 END) AS games_won
    FROM game_details
    GROUP BY GROUPING SETS (
        (player_name, team_abbreviation),
        (player_name),
        (team_abbreviation)
    )
)
select * from aggregated;


-- Who scored the most points playing for one team?
SELECT player_name, team_abbreviation, total_points
FROM aggregated_game_details
WHERE player_name IS NOT NULL AND team_abbreviation IS NOT NULL
ORDER BY total_points DESC
LIMIT 1;

-- Who scored the most points as a player?
SELECT player_name, total_points
FROM aggregated_game_details
WHERE player_name IS NOT NULL AND team_abbreviation IS NULL
ORDER BY total_points DESC
LIMIT 1;

-- Which team has won the most games?
SELECT team_abbreviation, games_won
FROM aggregated_game_details
WHERE team_abbreviation IS NOT NULL
ORDER BY games_won DESC
LIMIT 1;


--- Question3
--- Window Functions on game_details

-- Most games a team has won in a 90 game stretch
WITH team_games AS (
    SELECT
        team_abbreviation,
        game_id,
        SUM(CASE WHEN plus_minus > 0 THEN 1 ELSE 0 END) OVER (
            PARTITION BY team_abbreviation ORDER BY game_id
            ROWS BETWEEN 89 PRECEDING AND CURRENT ROW
        ) AS wins_in_90_games
    FROM game_details
)
SELECT
    team_abbreviation,
    MAX(wins_in_90_games) AS max_wins_in_90_games
FROM team_games
GROUP BY team_abbreviation
ORDER BY max_wins_in_90_games DESC
LIMIT 1;

-- Games in a row LeBron James scored over 10 points
WITH lebron_games AS (
    SELECT
        player_name,
        game_id,
        pts,
        CASE
            WHEN pts > 10 THEN 1 ELSE 0
        END AS scoring_streak,
        SUM(CASE WHEN pts > 10 THEN 1 ELSE 0 END) OVER (
            PARTITION BY player_name ORDER BY game_id
        ) AS streak_count
    FROM game_details
    WHERE player_name = 'LeBron James'
)
SELECT
    player_name,
    MAX(streak_count) AS max_scoring_streak
FROM lebron_games
GROUP BY player_name;