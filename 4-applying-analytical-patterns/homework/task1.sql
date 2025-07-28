
WITH player_first_season AS (
    SELECT
        player_name,
        MIN(current_season) as first_active_season
    FROM players
    WHERE is_active
    GROUP BY player_name
),

-- CTE to represent the "yesterday" state for all players and seasons
yesterday AS (
    SELECT * FROM players
),

-- CTE to represent the "today" state for all players and seasons
today AS (
    SELECT * FROM players
)

-- The main query uses the CTEs to perform the comparison
SELECT
    -- COALESCE is critical because a player might only exist on one side of the join
    COALESCE(today.player_name, yesterday.player_name) as player_name,
    -- The season we are reporting on is the "today" season
    COALESCE(today.current_season, yesterday.current_season + 1) as current_season,

    CASE
        -- CASE 1: Player exists today, but not yesterday (first season in data)
        WHEN yesterday.player_name IS NULL AND today.is_active THEN 'New'

        -- CASE 2: Player active today, was inactive yesterday
        WHEN today.is_active AND NOT yesterday.is_active THEN
            -- Check if it's their first-ever active season or a return
            CASE
                WHEN today.current_season = fs.first_active_season THEN 'New'
                ELSE 'Returned from Retirement'
            END

        -- CASE 3: Player active today and was active yesterday
        WHEN today.is_active AND yesterday.is_active THEN 'Continued Playing'

        -- CASE 4: Player NOT active today but was active yesterday
        WHEN (NOT today.is_active OR today.player_name IS NULL) AND yesterday.is_active THEN 'Retired'

        -- CASE 5: Player NOT active today and was NOT active yesterday
        WHEN NOT today.is_active AND NOT yesterday.is_active THEN 'Stayed Retired'

    END as player_state
FROM
    yesterday
-- Use a FULL OUTER JOIN to ensure we don't miss players who are new or who have retired
FULL OUTER JOIN today
    ON yesterday.player_name = today.player_name
    AND today.current_season = yesterday.current_season + 1
-- We still need the first_active_season to make the right decision
LEFT JOIN player_first_season fs
    ON COALESCE(today.player_name, yesterday.player_name) = fs.player_name
-- Clean up records that don't make sense (e.g., the season after the data ends)
WHERE
    COALESCE(today.current_season, yesterday.current_season + 1) <= (SELECT MAX(current_season) from players)
ORDER BY
    player_name,
    current_season;