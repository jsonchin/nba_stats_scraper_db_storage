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

DROP TABLE IF EXISTS starters;
CREATE TABLE starters AS
    SELECT SEASON, GAME_ID, PLAYER_ID, START_POSITION != "" AS IS_STARTER
        FROM game_info_traditional;

DROP TABLE IF EXISTS game_id_dates;
CREATE TABLE game_id_dates AS
    SELECT SEASON, GAME_ID, GAME_DATE
        FROM games
        GROUP BY
            games.SEASON,
            games.GAME_ID;

DROP TABLE IF EXISTS did_not_play;
CREATE TABLE did_not_play AS
    SELECT game_info_groups.SEASON,
        game_info_groups.GAME_ID AS GAME_ID,
        game_info_groups.TEAM_ID AS TEAM_ID,
        game_info_p_ids.PLAYER_ID AS PLAYER_ID,
        game_id_dates.GAME_DATE AS GAME_DATE
        FROM (SELECT game_info.SEASON,
                game_info.TEAM_ID,
                game_info.GAME_ID
                FROM game_info_traditional AS game_info
                GROUP BY
                    game_info.SEASON,
                    game_info.TEAM_ID,
                    game_info.GAME_ID) AS game_info_groups
            LEFT JOIN game_info_traditional AS game_info_p_ids
                ON game_info_p_ids.MIN IS NULL
                    AND game_info_groups.SEASON = game_info_p_ids.SEASON
                    AND game_info_groups.GAME_ID = game_info_p_ids.GAME_ID
                    AND game_info_groups.TEAM_ID = game_info_p_ids.TEAM_ID
            INNER JOIN game_id_dates
                ON game_id_dates.GAME_ID = game_info_groups.GAME_ID
                    AND game_id_dates.SEASON = game_info_groups.SEASON;

DROP TABLE IF EXISTS dnp_stats;
CREATE TABLE dnp_stats AS
    SELECT
        COALESCE(SUM(p_avg_stats_dnp.MIN), 0) AS DNP_MIN,
        COALESCE(SUM(p_avg_stats_dnp.FGM), 0) AS DNP_FGM,
        COALESCE(SUM(p_avg_stats_dnp.FGA), 0) AS DNP_FGA,
        COALESCE(SUM(p_avg_stats_dnp.FG3M), 0) AS DNP_FG3M,
        COALESCE(SUM(p_avg_stats_dnp.FG3A), 0) AS DNP_FG3A,
        COALESCE(SUM(p_avg_stats_dnp.FTM), 0) AS DNP_FTM,
        COALESCE(SUM(p_avg_stats_dnp.FTA), 0) AS DNP_FTA,
        COALESCE(SUM(p_avg_stats_dnp.OREB), 0) AS DNP_OREB,
        COALESCE(SUM(p_avg_stats_dnp.DREB), 0) AS DNP_DREB,
        COALESCE(SUM(p_avg_stats_dnp.REB), 0) AS DNP_REB,
        COALESCE(SUM(p_avg_stats_dnp.AST), 0)AS DNP_AST,
        COALESCE(SUM(p_avg_stats_dnp.TOV), 0) AS DNP_TOV,
        COALESCE(SUM(p_avg_stats_dnp.STL), 0) AS DNP_STL,
        COALESCE(SUM(p_avg_stats_dnp.BLK), 0) AS DNP_BLK,
        COALESCE(SUM(p_avg_stats_dnp.BLKA), 0) AS DNP_BLKA,
        COALESCE(SUM(p_avg_stats_dnp.PF), 0) AS DNP_PF,
        COALESCE(SUM(p_avg_stats_dnp.PFD), 0) AS DNP_PFD,
        COALESCE(SUM(p_avg_stats_dnp.PTS), 0) AS DNP_PTS,
        COALESCE(SUM(p_avg_stats_dnp.PLUS_MINUS), 0) AS DNP_PLUS_MINUS,
        COALESCE(SUM(p_avg_stats_dnp.NBA_FANTASY_PTS), 0) AS DNP_NBA_FANTASY_PTS,
        COALESCE(SUM(p_avg_stats_dnp.DD2), 0) AS DNP_DD2,
        COALESCE(SUM(p_avg_stats_dnp.TD3), 0) AS DNP_TD3,
        dnp.GAME_ID AS GAME_ID,
        dnp.TEAM_ID AS TEAM_ID,
        dnp.SEASON AS SEASON
        FROM DID_NOT_PLAY AS dnp
            LEFT JOIN GENERAL_TRADITIONAL_PLAYER_STATS AS p_avg_stats_dnp
                ON dnp.PLAYER_ID = p_avg_stats_dnp.PLAYER_ID
                    AND dnp.SEASON = p_avg_stats_dnp.SEASON
                    AND DATE(dnp.GAME_DATE, '-1 day') = p_avg_stats_dnp.DATE_TO
        GROUP BY
            dnp.SEASON,
            dnp.GAME_ID,
            dnp.TEAM_ID;
