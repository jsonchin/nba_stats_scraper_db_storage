DROP VIEW IF EXISTS team_abbreviations;
CREATE VIEW team_abbreviations AS
    SELECT SEASON, TEAM_ID, TEAM_ABBREVIATION
        FROM games
        GROUP BY SEASON, TEAM_ID;

DROP VIEW IF EXISTS general_team_stats_opponent_abbrevs;
CREATE VIEW general_team_stats_opponent_abbrevs AS
    SELECT general_team_stats_opponent.*, TEAM_ABBREVIATION
        FROM general_team_stats_opponent
            INNER JOIN team_abbreviations
                ON
                    general_team_stats_opponent.SEASON = team_abbreviations.SEASON
                    AND general_team_stats_opponent.TEAM_ID = team_abbreviations.TEAM_ID;

DROP VIEW IF EXISTS yesterday_date_map;
CREATE VIEW yesterday_date_map AS
    SELECT p_log1.SEASON AS SEASON,
            p_log1.PLAYER_ID AS PLAYER_ID,
            p_log1.GAME_DATE AS PAST_GAME_DATE,
            p_log2.GAME_DATE AS FUTURE_GAME_DATE
        FROM player_logs AS p_log1,
            player_logs AS p_log2
        WHERE p_log1.SEASON = p_log2.SEASON
            AND p_log1.PLAYER_ID = p_log2.PLAYER_ID
            AND p_log2.GAME_DATE = (SELECT MIN(inner_player_logs.GAME_DATE)
                            FROM player_logs AS inner_player_logs
                            WHERE inner_player_logs.SEASON = p_log1.SEASON
                                AND inner_player_logs.PLAYER_ID = p_log1.PLAYER_ID
                                AND inner_player_logs.GAME_DATE > p_log1.GAME_DATE
                            GROUP BY inner_player_logs.PLAYER_ID);