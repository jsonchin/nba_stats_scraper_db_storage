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

DROP VIEW IF EXISTS general_team_stats_abbrevs;
CREATE VIEW general_team_stats_abbrevs AS
    SELECT general_team_stats.*, TEAM_ABBREVIATION
        FROM general_team_stats
            INNER JOIN team_abbreviations
                ON
                    general_team_stats.SEASON = team_abbreviations.SEASON
                    AND general_team_stats.TEAM_ID = team_abbreviations.TEAM_ID;

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

CREATE TEMP TABLE PLAYER_LOG_STARTERS AS
            SELECT
                p_log.*, IS_STARTER
            FROM PLAYER_LOGS AS p_log
                INNER JOIN STARTERS AS starters
                    ON p_log.SEASON = starters.SEASON
                        AND p_log.GAME_ID = starters.GAME_ID
                        AND p_log.PLAYER_ID = starters.PLAYER_ID;

CREATE TEMP TABLE PLAYER_BENCH_STARTER_AVGS AS
            SELECT
                AVG(p_log_starters.MIN) AS AVG_BENCH_STARTER_MIN,
                AVG(p_log_starters.FGM) AS AVG_BENCH_STARTER_FGM,
                AVG(p_log_starters.FGA) AS AVG_BENCH_STARTER_FGA,
                AVG(p_log_starters.FG_PCT) AS AVG_BENCH_STARTER_FG_PCT,
                AVG(p_log_starters.FG3M) AS AVG_BENCH_STARTER_FG3M,
                AVG(p_log_starters.FG3A) AS AVG_BENCH_STARTER_FG3A,
                AVG(p_log_starters.FG3_PCT) AS AVG_BENCH_STARTER_FG3_PCT,
                AVG(p_log_starters.FTM) AS AVG_BENCH_STARTER_FTM,
                AVG(p_log_starters.FTA) AS AVG_BENCH_STARTER_FTA,
                AVG(p_log_starters.FT_PCT) AS AVG_BENCH_STARTER_FT_PCT,
                AVG(p_log_starters.OREB) AS AVG_BENCH_STARTER_OREB,
                AVG(p_log_starters.DREB) AS AVG_BENCH_STARTER_DREB,
                AVG(p_log_starters.REB) AS AVG_BENCH_STARTER_REB,
                AVG(p_log_starters.AST) AS AVG_BENCH_STARTER_AST,
                AVG(p_log_starters.TOV) AS AVG_BENCH_STARTER_TOV,
                AVG(p_log_starters.STL) AS AVG_BENCH_STARTER_STL,
                AVG(p_log_starters.BLK) AS AVG_BENCH_STARTER_BLK,
                AVG(p_log_starters.BLKA) AS AVG_BENCH_STARTER_BLKA,
                AVG(p_log_starters.PF) AS AVG_BENCH_STARTER_PF,
                AVG(p_log_starters.PFD) AS AVG_BENCH_STARTER_PFD,
                AVG(p_log_starters.PTS) AS AVG_BENCH_STARTER_PTS,
                AVG(p_log_starters.PLUS_MINUS) AS AVG_BENCH_STARTER_PLUS_MINUS,
                AVG(p_log_starters.NBA_FANTASY_PTS) AS AVG_BENCH_STARTER_NBA_FANTASY_PTS,
                AVG(p_log_starters.DD2) AS AVG_BENCH_STARTER_DD2,
                AVG(p_log_starters.TD3) AS AVG_BENCH_STARTER_TD3,
                p_log.IS_STARTER AS IS_STARTER,
                p_log.GAME_DATE AS GAME_DATE,
                p_log.PLAYER_ID AS PLAYER_ID,
                p_log.SEASON AS SEASON
            FROM PLAYER_LOG_STARTERS AS p_log
                INNER JOIN PLAYER_LOG_STARTERS AS p_log_starters
                    ON p_log.PLAYER_ID = p_log_starters.PLAYER_ID
                        AND p_log.GAME_DATE > p_log_starters.GAME_DATE
                        AND p_log.SEASON = p_log_starters.SEASON
                        AND p_log.IS_STARTER = p_log_starters.IS_STARTER
            GROUP BY p_log.SEASON, p_log.GAME_DATE, p_log.PLAYER_ID, p_log.IS_STARTER;


CREATE TEMP TABLE SIGNIFICANT_DID_NOT_PLAY AS
            SELECT
                dnp.*
            FROM DID_NOT_PLAY AS dnp
                INNER JOIN GENERAL_TRADITIONAL_PLAYER_STATS AS p_avg_stats
                    ON p_avg_stats.PLAYER_ID = dnp.PLAYER_ID
                        AND p_avg_stats.SEASON = dnp.SEASON
                        AND p_avg_stats.DATE_TO = DATE(dnp.GAME_DATE, '-1 day')
                        AND dnp.PLAYER_ID IS NOT NULL
                        AND p_avg_stats.MIN > 0;

CREATE TEMP TABLE PLAYER_OFF_PLAYER AS
            SELECT
                p_log.SEASON, p_log.GAME_ID, p_log.TEAM_ID, p_log.GAME_DATE AS GAME_DATE,
                p_log.PLAYER_ID, dnp.PLAYER_ID AS INJURED_PLAYER_ID

            FROM PLAYER_LOGS AS p_log
                INNER JOIN SIGNIFICANT_DID_NOT_PLAY AS dnp
                    ON p_log.GAME_ID = dnp.GAME_ID
                        AND p_log.SEASON = p_log.SEASON
                        AND p_log.TEAM_ID = dnp.TEAM_ID
                        AND dnp.PLAYER_ID IS NOT NULL;

CREATE TEMP TABLE PLAYER_INJURED_FOR_TEAM AS
            SELECT SEASON, TEAM_ID, PLAYER_ID, GAME_ID
                FROM SIGNIFICANT_DID_NOT_PLAY
                WHERE PLAYER_ID IS NOT NULL
                GROUP BY SEASON, TEAM_ID, PLAYER_ID;

CREATE TEMP TABLE PLAYER_BOTH_ON_OFF_PLAYER AS
            SELECT
                p_log.SEASON, p_log.GAME_ID, p_log.TEAM_ID, p_log.GAME_DATE AS GAME_DATE,
                p_log.PLAYER_ID, dnp.PLAYER_ID AS INJURED_PLAYER_ID
            FROM PLAYER_LOGS AS p_log
                INNER JOIN PLAYER_INJURED_FOR_TEAM AS dnp
                    ON p_log.SEASON = dnp.SEASON
                        AND p_log.TEAM_ID = dnp.TEAM_ID;

CREATE TEMP TABLE PLAYER_ON_PLAYER AS

    SELECT *
        FROM PLAYER_BOTH_ON_OFF_PLAYER
        EXCEPT
            SELECT * FROM PLAYER_OFF_PLAYER;
DROP TABLE PLAYER_BOTH_ON_OFF_PLAYER;

CREATE TEMP TABLE PLAYER_OFF_PLAYER_STATS AS
            SELECT
                p_log.*,
                adv_p_log.OFF_RATING AS OFF_RATING,
                adv_p_log.DEF_RATING AS DEF_RATING,
                adv_p_log.NET_RATING AS NET_RATING,
                adv_p_log.AST_PCT AS AST_PCT,
                adv_p_log.AST_TO AS AST_TO,
                adv_p_log.AST_RATIO AS AST_RATIO,
                adv_p_log.OREB_PCT AS OREB_PCT,
                adv_p_log.DREB_PCT AS DREB_PCT,
                adv_p_log.REB_PCT AS REB_PCT,
                adv_p_log.TM_TOV_PCT AS TM_TOV_PCT,
                adv_p_log.EFG_PCT AS EFG_PCT,
                adv_p_log.TS_PCT AS TS_PCT,
                adv_p_log.USG_PCT AS USG_PCT,
                adv_p_log.PACE AS PACE,
                adv_p_log.PIE AS PIE,
                adv_p_log.FGM_PG AS FGM_PG,
                adv_p_log.FGA_PG AS FGA_PG,
                p_off.INJURED_PLAYER_ID

            FROM PLAYER_OFF_PLAYER AS p_off
                INNER JOIN PLAYER_LOGS AS p_log
                    ON p_off.SEASON = p_log.SEASON
                        AND p_off.PLAYER_ID = p_log.PLAYER_ID
                        AND p_off.GAME_ID = p_log.GAME_ID
                INNER JOIN PLAYER_LOGS_ADVANCED AS adv_p_log
                    ON adv_p_log.SEASON = p_log.SEASON
                        AND adv_p_log.PLAYER_ID = p_log.PLAYER_ID
                        AND adv_p_log.GAME_ID = p_log.GAME_ID;
DROP TABLE PLAYER_OFF_PLAYER;

CREATE TEMP TABLE PLAYER_ON_PLAYER_STATS AS
            SELECT
                p_log.*,
                adv_p_log.OFF_RATING AS OFF_RATING,
                adv_p_log.DEF_RATING AS DEF_RATING,
                adv_p_log.NET_RATING AS NET_RATING,
                adv_p_log.AST_PCT AS AST_PCT,
                adv_p_log.AST_TO AS AST_TO,
                adv_p_log.AST_RATIO AS AST_RATIO,
                adv_p_log.OREB_PCT AS OREB_PCT,
                adv_p_log.DREB_PCT AS DREB_PCT,
                adv_p_log.REB_PCT AS REB_PCT,
                adv_p_log.TM_TOV_PCT AS TM_TOV_PCT,
                adv_p_log.EFG_PCT AS EFG_PCT,
                adv_p_log.TS_PCT AS TS_PCT,
                adv_p_log.USG_PCT AS USG_PCT,
                adv_p_log.PACE AS PACE,
                adv_p_log.PIE AS PIE,
                adv_p_log.FGM_PG AS FGM_PG,
                adv_p_log.FGA_PG AS FGA_PG,
                p_on.INJURED_PLAYER_ID

            FROM PLAYER_ON_PLAYER AS p_on
                INNER JOIN PLAYER_LOGS AS p_log
                    ON p_on.SEASON = p_log.SEASON
                        AND p_on.PLAYER_ID = p_log.PLAYER_ID
                        AND p_on.GAME_ID = p_log.GAME_ID
                INNER JOIN PLAYER_LOGS_ADVANCED AS adv_p_log
                    ON adv_p_log.SEASON = p_log.SEASON
                        AND adv_p_log.PLAYER_ID = p_log.PLAYER_ID
                        AND adv_p_log.GAME_ID = p_log.GAME_ID;
DROP TABLE PLAYER_ON_PLAYER;

CREATE TEMP TABLE AVG_PLAYER_WHEN_PLAYER_OFF AS
            SELECT AVG(player_injured_pairs.MIN) AS AVG_OFF_MIN,
                    AVG(player_injured_pairs.FGM) AS AVG_OFF_FGM,
                    AVG(player_injured_pairs.FGA) AS AVG_OFF_FGA,
                    AVG(player_injured_pairs.FG_PCT) AS AVG_OFF_FG_PCT,
                    AVG(player_injured_pairs.FG3M) AS AVG_OFF_FG3M,
                    AVG(player_injured_pairs.FG3A) AS AVG_OFF_FG3A,
                    AVG(player_injured_pairs.FG3_PCT) AS AVG_OFF_FG3_PCT,
                    AVG(player_injured_pairs.FTM) AS AVG_OFF_FTM,
                    AVG(player_injured_pairs.FTA) AS AVG_OFF_FTA,
                    AVG(player_injured_pairs.FT_PCT) AS AVG_OFF_FT_PCT,
                    AVG(player_injured_pairs.OREB) AS AVG_OFF_OREB,
                    AVG(player_injured_pairs.DREB) AS AVG_OFF_DREB,
                    AVG(player_injured_pairs.REB) AS AVG_OFF_REB,
                    AVG(player_injured_pairs.AST) AS AVG_OFF_AST,
                    AVG(player_injured_pairs.TOV) AS AVG_OFF_TOV,
                    AVG(player_injured_pairs.STL) AS AVG_OFF_STL,
                    AVG(player_injured_pairs.BLK) AS AVG_OFF_BLK,
                    AVG(player_injured_pairs.BLKA) AS AVG_OFF_BLKA,
                    AVG(player_injured_pairs.PF) AS AVG_OFF_PF,
                    AVG(player_injured_pairs.PFD) AS AVG_OFF_PFD,
                    AVG(player_injured_pairs.PTS) AS AVG_OFF_PTS,
                    AVG(player_injured_pairs.PLUS_MINUS) AS AVG_OFF_PLUS_MINUS,
                    AVG(player_injured_pairs.NBA_FANTASY_PTS) AS AVG_OFF_NBA_FANTASY_PTS,
                    AVG(player_injured_pairs.DD2) AS AVG_OFF_DD2,
                    AVG(player_injured_pairs.TD3) AS AVG_OFF_TD3,
                    AVG(player_injured_pairs.OFF_RATING) AS AVG_OFF_OFF_RATING,
                    AVG(player_injured_pairs.DEF_RATING) AS AVG_OFF_DEF_RATING,
                    AVG(player_injured_pairs.NET_RATING) AS AVG_OFF_NET_RATING,
                    AVG(player_injured_pairs.AST_PCT) AS AVG_OFF_AST_PCT,
                    AVG(player_injured_pairs.AST_TO) AS AVG_OFF_AST_TO,
                    AVG(player_injured_pairs.AST_RATIO) AS AVG_OFF_AST_RATIO,
                    AVG(player_injured_pairs.OREB_PCT) AS AVG_OFF_OREB_PCT,
                    AVG(player_injured_pairs.DREB_PCT) AS AVG_OFF_DREB_PCT,
                    AVG(player_injured_pairs.REB_PCT) AS AVG_OFF_REB_PCT,
                    AVG(player_injured_pairs.TM_TOV_PCT) AS AVG_OFF_TM_TOV_PCT,
                    AVG(player_injured_pairs.EFG_PCT) AS AVG_OFF_EFG_PCT,
                    AVG(player_injured_pairs.TS_PCT) AS AVG_OFF_TS_PCT,
                    AVG(player_injured_pairs.USG_PCT) AS AVG_OFF_USG_PCT,
                    AVG(player_injured_pairs.PACE) AS AVG_OFF_PACE,
                    AVG(player_injured_pairs.PIE) AS AVG_OFF_PIE,
                    AVG(player_injured_pairs.FGM_PG) AS AVG_OFF_FGM_PG,
                    AVG(player_injured_pairs.FGA_PG) AS AVG_OFF_FGA_PG,
                    COUNT(*) AS NUM_GAMES_PLAYED_WITH_INJURED,
                    p_log.PLAYER_ID AS PLAYER_ID,
                    player_injured_pairs.INJURED_PLAYER_ID AS INJURED_PLAYER_ID,
                    p_log.GAME_DATE AS GAME_DATE,
                    p_log.SEASON AS SEASON

            FROM PLAYER_OFF_PLAYER_STATS AS p_log
                INNER JOIN PLAYER_OFF_PLAYER_STATS AS player_injured_pairs
                    ON p_log.SEASON = player_injured_pairs.SEASON
                        AND p_log.TEAM_ID = player_injured_pairs.TEAM_ID
                        AND p_log.PLAYER_ID = player_injured_pairs.PLAYER_ID
                        AND p_log.GAME_DATE > player_injured_pairs.GAME_DATE
                        AND p_log.INJURED_PLAYER_ID = player_injured_pairs.INJURED_PLAYER_ID
                GROUP BY p_log.PLAYER_ID, player_injured_pairs.INJURED_PLAYER_ID,
                        p_log.SEASON, p_log.TEAM_ID, p_log.GAME_DATE;

CREATE TEMP TABLE AVG_PLAYER_WHEN_PLAYER_ON AS
            SELECT AVG(player_injured_pairs.MIN) AS AVG_ON_MIN,
                    AVG(player_injured_pairs.FGM) AS AVG_ON_FGM,
                    AVG(player_injured_pairs.FGA) AS AVG_ON_FGA,
                    AVG(player_injured_pairs.FG_PCT) AS AVG_ON_FG_PCT,
                    AVG(player_injured_pairs.FG3M) AS AVG_ON_FG3M,
                    AVG(player_injured_pairs.FG3A) AS AVG_ON_FG3A,
                    AVG(player_injured_pairs.FG3_PCT) AS AVG_ON_FG3_PCT,
                    AVG(player_injured_pairs.FTM) AS AVG_ON_FTM,
                    AVG(player_injured_pairs.FTA) AS AVG_ON_FTA,
                    AVG(player_injured_pairs.FT_PCT) AS AVG_ON_FT_PCT,
                    AVG(player_injured_pairs.OREB) AS AVG_ON_OREB,
                    AVG(player_injured_pairs.DREB) AS AVG_ON_DREB,
                    AVG(player_injured_pairs.REB) AS AVG_ON_REB,
                    AVG(player_injured_pairs.AST) AS AVG_ON_AST,
                    AVG(player_injured_pairs.TOV) AS AVG_ON_TOV,
                    AVG(player_injured_pairs.STL) AS AVG_ON_STL,
                    AVG(player_injured_pairs.BLK) AS AVG_ON_BLK,
                    AVG(player_injured_pairs.BLKA) AS AVG_ON_BLKA,
                    AVG(player_injured_pairs.PF) AS AVG_ON_PF,
                    AVG(player_injured_pairs.PFD) AS AVG_ON_PFD,
                    AVG(player_injured_pairs.PTS) AS AVG_ON_PTS,
                    AVG(player_injured_pairs.PLUS_MINUS) AS AVG_ON_PLUS_MINUS,
                    AVG(player_injured_pairs.NBA_FANTASY_PTS) AS AVG_ON_NBA_FANTASY_PTS,
                    AVG(player_injured_pairs.DD2) AS AVG_ON_DD2,
                    AVG(player_injured_pairs.TD3) AS AVG_ON_TD3,
                    AVG(player_injured_pairs.OFF_RATING) AS AVG_ON_OFF_RATING,
                    AVG(player_injured_pairs.DEF_RATING) AS AVG_ON_DEF_RATING,
                    AVG(player_injured_pairs.NET_RATING) AS AVG_ON_NET_RATING,
                    AVG(player_injured_pairs.AST_PCT) AS AVG_ON_AST_PCT,
                    AVG(player_injured_pairs.AST_TO) AS AVG_ON_AST_TO,
                    AVG(player_injured_pairs.AST_RATIO) AS AVG_ON_AST_RATIO,
                    AVG(player_injured_pairs.OREB_PCT) AS AVG_ON_OREB_PCT,
                    AVG(player_injured_pairs.DREB_PCT) AS AVG_ON_DREB_PCT,
                    AVG(player_injured_pairs.REB_PCT) AS AVG_ON_REB_PCT,
                    AVG(player_injured_pairs.TM_TOV_PCT) AS AVG_ON_TM_TOV_PCT,
                    AVG(player_injured_pairs.EFG_PCT) AS AVG_ON_EFG_PCT,
                    AVG(player_injured_pairs.TS_PCT) AS AVG_ON_TS_PCT,
                    AVG(player_injured_pairs.USG_PCT) AS AVG_ON_USG_PCT,
                    AVG(player_injured_pairs.PACE) AS AVG_ON_PACE,
                    AVG(player_injured_pairs.PIE) AS AVG_ON_PIE,
                    AVG(player_injured_pairs.FGM_PG) AS AVG_ON_FGM_PG,
                    AVG(player_injured_pairs.FGA_PG) AS AVG_ON_FGA_PG,
                    COUNT(*) AS NUM_GAMES_PLAYED_WITH_INJURED,
                    p_log.PLAYER_ID AS PLAYER_ID,
                    player_injured_pairs.INJURED_PLAYER_ID AS INJURED_PLAYER_ID,
                    p_log.GAME_DATE AS GAME_DATE,
                    p_log.SEASON AS SEASON

            FROM AVG_PLAYER_WHEN_PLAYER_OFF AS p_log
                INNER JOIN PLAYER_ON_PLAYER_STATS AS player_injured_pairs
                    ON p_log.SEASON = player_injured_pairs.SEASON
                        AND p_log.PLAYER_ID = player_injured_pairs.PLAYER_ID
                        AND p_log.GAME_DATE > player_injured_pairs.GAME_DATE
                        AND p_log.INJURED_PLAYER_ID = player_injured_pairs.INJURED_PLAYER_ID
                GROUP BY p_log.PLAYER_ID, player_injured_pairs.INJURED_PLAYER_ID,
                        p_log.SEASON, p_log.GAME_DATE;

CREATE TEMP TABLE MAX_PLAYER_OFF_DIFF AS
        SELECT MAX(AVG_OFF_MIN) AS MAX_AVG_OFF_MIN,
                MAX(AVG_OFF_FGM) AS MAX_AVG_OFF_FGM,
                MAX(AVG_OFF_FGA) AS MAX_AVG_OFF_FGA,
                MAX(AVG_OFF_FG_PCT) AS MAX_AVG_OFF_FG_PCT,
                MAX(AVG_OFF_FG3M) AS MAX_AVG_OFF_FG3M,
                MAX(AVG_OFF_FG3A) AS MAX_AVG_OFF_FG3A,
                MAX(AVG_OFF_FG3_PCT) AS MAX_AVG_OFF_FG3_PCT,
                MAX(AVG_OFF_FTM) AS MAX_AVG_OFF_FTM,
                MAX(AVG_OFF_FTA) AS MAX_AVG_OFF_FTA,
                MAX(AVG_OFF_FT_PCT) AS MAX_AVG_OFF_FT_PCT,
                MAX(AVG_OFF_OREB) AS MAX_AVG_OFF_OREB,
                MAX(AVG_OFF_DREB) AS MAX_AVG_OFF_DREB,
                MAX(AVG_OFF_REB) AS MAX_AVG_OFF_REB,
                MAX(AVG_OFF_AST) AS MAX_AVG_OFF_AST,
                MAX(AVG_OFF_TOV) AS MAX_AVG_OFF_TOV,
                MAX(AVG_OFF_STL) AS MAX_AVG_OFF_STL,
                MAX(AVG_OFF_BLK) AS MAX_AVG_OFF_BLK,
                MAX(AVG_OFF_BLKA) AS MAX_AVG_OFF_BLKA,
                MAX(AVG_OFF_PF) AS MAX_AVG_OFF_PF,
                MAX(AVG_OFF_PFD) AS MAX_AVG_OFF_PFD,
                MAX(AVG_OFF_PTS) AS MAX_AVG_OFF_PTS,
                MAX(AVG_OFF_PLUS_MINUS) AS MAX_AVG_OFF_PLUS_MINUS,
                MAX(AVG_OFF_NBA_FANTASY_PTS) AS MAX_AVG_OFF_NBA_FANTASY_PTS,
                MAX(AVG_OFF_DD2) AS MAX_AVG_OFF_DD2,
                MAX(AVG_OFF_TD3) AS MAX_AVG_OFF_TD3,
                MAX(AVG_OFF_OFF_RATING) AS MAX_AVG_OFF_OFF_RATING,
                MAX(AVG_OFF_DEF_RATING) AS MAX_AVG_OFF_DEF_RATING,
                MAX(AVG_OFF_NET_RATING) AS MAX_AVG_OFF_NET_RATING,
                MAX(AVG_OFF_AST_PCT) AS MAX_AVG_OFF_AST_PCT,
                MAX(AVG_OFF_AST_TO) AS MAX_AVG_OFF_AST_TO,
                MAX(AVG_OFF_AST_RATIO) AS MAX_AVG_OFF_AST_RATIO,
                MAX(AVG_OFF_OREB_PCT) AS MAX_AVG_OFF_OREB_PCT,
                MAX(AVG_OFF_DREB_PCT) AS MAX_AVG_OFF_DREB_PCT,
                MAX(AVG_OFF_REB_PCT) AS MAX_AVG_OFF_REB_PCT,
                MAX(AVG_OFF_TM_TOV_PCT) AS MAX_AVG_OFF_TM_TOV_PCT,
                MAX(AVG_OFF_EFG_PCT) AS MAX_AVG_OFF_EFG_PCT,
                MAX(AVG_OFF_TS_PCT) AS MAX_AVG_OFF_TS_PCT,
                MAX(AVG_OFF_USG_PCT) AS MAX_AVG_OFF_USG_PCT,
                MAX(AVG_OFF_PACE) AS MAX_AVG_OFF_PACE,
                MAX(AVG_OFF_PIE) AS MAX_AVG_OFF_PIE,
                MAX(AVG_OFF_FGM_PG) AS MAX_AVG_OFF_FGM_PG,
                MAX(AVG_OFF_FGA_PG) AS MAX_AVG_OFF_FGA_PG,

                PLAYER_ID, GAME_DATE, SEASON
            FROM AVG_PLAYER_WHEN_PLAYER_OFF
                GROUP BY PLAYER_ID, GAME_DATE, SEASON;

CREATE TEMP TABLE SUM_PLAYER_OFF_DIFF AS
        SELECT SUM(AVG_OFF_MIN) AS SUM_AVG_OFF_MIN,
                SUM(AVG_OFF_FGM) AS SUM_AVG_OFF_FGM,
                SUM(AVG_OFF_FGA) AS SUM_AVG_OFF_FGA,
                SUM(AVG_OFF_FG_PCT) AS SUM_AVG_OFF_FG_PCT,
                SUM(AVG_OFF_FG3M) AS SUM_AVG_OFF_FG3M,
                SUM(AVG_OFF_FG3A) AS SUM_AVG_OFF_FG3A,
                SUM(AVG_OFF_FG3_PCT) AS SUM_AVG_OFF_FG3_PCT,
                SUM(AVG_OFF_FTM) AS SUM_AVG_OFF_FTM,
                SUM(AVG_OFF_FTA) AS SUM_AVG_OFF_FTA,
                SUM(AVG_OFF_FT_PCT) AS SUM_AVG_OFF_FT_PCT,
                SUM(AVG_OFF_OREB) AS SUM_AVG_OFF_OREB,
                SUM(AVG_OFF_DREB) AS SUM_AVG_OFF_DREB,
                SUM(AVG_OFF_REB) AS SUM_AVG_OFF_REB,
                SUM(AVG_OFF_AST) AS SUM_AVG_OFF_AST,
                SUM(AVG_OFF_TOV) AS SUM_AVG_OFF_TOV,
                SUM(AVG_OFF_STL) AS SUM_AVG_OFF_STL,
                SUM(AVG_OFF_BLK) AS SUM_AVG_OFF_BLK,
                SUM(AVG_OFF_BLKA) AS SUM_AVG_OFF_BLKA,
                SUM(AVG_OFF_PF) AS SUM_AVG_OFF_PF,
                SUM(AVG_OFF_PFD) AS SUM_AVG_OFF_PFD,
                SUM(AVG_OFF_PTS) AS SUM_AVG_OFF_PTS,
                SUM(AVG_OFF_PLUS_MINUS) AS SUM_AVG_OFF_PLUS_MINUS,
                SUM(AVG_OFF_NBA_FANTASY_PTS) AS SUM_AVG_OFF_NBA_FANTASY_PTS,
                SUM(AVG_OFF_DD2) AS SUM_AVG_OFF_DD2,
                SUM(AVG_OFF_TD3) AS SUM_AVG_OFF_TD3,
                SUM(AVG_OFF_OFF_RATING) AS SUM_AVG_OFF_OFF_RATING,
                SUM(AVG_OFF_DEF_RATING) AS SUM_AVG_OFF_DEF_RATING,
                SUM(AVG_OFF_NET_RATING) AS SUM_AVG_OFF_NET_RATING,
                SUM(AVG_OFF_AST_PCT) AS SUM_AVG_OFF_AST_PCT,
                SUM(AVG_OFF_AST_TO) AS SUM_AVG_OFF_AST_TO,
                SUM(AVG_OFF_AST_RATIO) AS SUM_AVG_OFF_AST_RATIO,
                SUM(AVG_OFF_OREB_PCT) AS SUM_AVG_OFF_OREB_PCT,
                SUM(AVG_OFF_DREB_PCT) AS SUM_AVG_OFF_DREB_PCT,
                SUM(AVG_OFF_REB_PCT) AS SUM_AVG_OFF_REB_PCT,
                SUM(AVG_OFF_TM_TOV_PCT) AS SUM_AVG_OFF_TM_TOV_PCT,
                SUM(AVG_OFF_EFG_PCT) AS SUM_AVG_OFF_EFG_PCT,
                SUM(AVG_OFF_TS_PCT) AS SUM_AVG_OFF_TS_PCT,
                SUM(AVG_OFF_USG_PCT) AS SUM_AVG_OFF_USG_PCT,
                SUM(AVG_OFF_PACE) AS SUM_AVG_OFF_PACE,
                SUM(AVG_OFF_PIE) AS SUM_AVG_OFF_PIE,
                SUM(AVG_OFF_FGM_PG) AS SUM_AVG_OFF_FGM_PG,
                SUM(AVG_OFF_FGA_PG) AS SUM_AVG_OFF_FGA_PG,

                PLAYER_ID, GAME_DATE, SEASON
            FROM AVG_PLAYER_WHEN_PLAYER_OFF
                GROUP BY PLAYER_ID, GAME_DATE, SEASON;

CREATE TEMP TABLE AVG_PLAYER_OFF_DIFF AS
        SELECT AVG(AVG_OFF_MIN) AS AVG_AVG_OFF_MIN,
                AVG(AVG_OFF_FGM) AS AVG_AVG_OFF_FGM,
                AVG(AVG_OFF_FGA) AS AVG_AVG_OFF_FGA,
                AVG(AVG_OFF_FG_PCT) AS AVG_AVG_OFF_FG_PCT,
                AVG(AVG_OFF_FG3M) AS AVG_AVG_OFF_FG3M,
                AVG(AVG_OFF_FG3A) AS AVG_AVG_OFF_FG3A,
                AVG(AVG_OFF_FG3_PCT) AS AVG_AVG_OFF_FG3_PCT,
                AVG(AVG_OFF_FTM) AS AVG_AVG_OFF_FTM,
                AVG(AVG_OFF_FTA) AS AVG_AVG_OFF_FTA,
                AVG(AVG_OFF_FT_PCT) AS AVG_AVG_OFF_FT_PCT,
                AVG(AVG_OFF_OREB) AS AVG_AVG_OFF_OREB,
                AVG(AVG_OFF_DREB) AS AVG_AVG_OFF_DREB,
                AVG(AVG_OFF_REB) AS AVG_AVG_OFF_REB,
                AVG(AVG_OFF_AST) AS AVG_AVG_OFF_AST,
                AVG(AVG_OFF_TOV) AS AVG_AVG_OFF_TOV,
                AVG(AVG_OFF_STL) AS AVG_AVG_OFF_STL,
                AVG(AVG_OFF_BLK) AS AVG_AVG_OFF_BLK,
                AVG(AVG_OFF_BLKA) AS AVG_AVG_OFF_BLKA,
                AVG(AVG_OFF_PF) AS AVG_AVG_OFF_PF,
                AVG(AVG_OFF_PFD) AS AVG_AVG_OFF_PFD,
                AVG(AVG_OFF_PTS) AS AVG_AVG_OFF_PTS,
                AVG(AVG_OFF_PLUS_MINUS) AS AVG_AVG_OFF_PLUS_MINUS,
                AVG(AVG_OFF_NBA_FANTASY_PTS) AS AVG_AVG_OFF_NBA_FANTASY_PTS,
                AVG(AVG_OFF_DD2) AS AVG_AVG_OFF_DD2,
                AVG(AVG_OFF_TD3) AS AVG_AVG_OFF_TD3,
                AVG(AVG_OFF_OFF_RATING) AS AVG_AVG_OFF_OFF_RATING,
                AVG(AVG_OFF_DEF_RATING) AS AVG_AVG_OFF_DEF_RATING,
                AVG(AVG_OFF_NET_RATING) AS AVG_AVG_OFF_NET_RATING,
                AVG(AVG_OFF_AST_PCT) AS AVG_AVG_OFF_AST_PCT,
                AVG(AVG_OFF_AST_TO) AS AVG_AVG_OFF_AST_TO,
                AVG(AVG_OFF_AST_RATIO) AS AVG_AVG_OFF_AST_RATIO,
                AVG(AVG_OFF_OREB_PCT) AS AVG_AVG_OFF_OREB_PCT,
                AVG(AVG_OFF_DREB_PCT) AS AVG_AVG_OFF_DREB_PCT,
                AVG(AVG_OFF_REB_PCT) AS AVG_AVG_OFF_REB_PCT,
                AVG(AVG_OFF_TM_TOV_PCT) AS AVG_AVG_OFF_TM_TOV_PCT,
                AVG(AVG_OFF_EFG_PCT) AS AVG_AVG_OFF_EFG_PCT,
                AVG(AVG_OFF_TS_PCT) AS AVG_AVG_OFF_TS_PCT,
                AVG(AVG_OFF_USG_PCT) AS AVG_AVG_OFF_USG_PCT,
                AVG(AVG_OFF_PACE) AS AVG_AVG_OFF_PACE,
                AVG(AVG_OFF_PIE) AS AVG_AVG_OFF_PIE,
                AVG(AVG_OFF_FGM_PG) AS AVG_AVG_OFF_FGM_PG,
                AVG(AVG_OFF_FGA_PG) AS AVG_AVG_OFF_FGA_PG,

                PLAYER_ID, GAME_DATE, SEASON
            FROM AVG_PLAYER_WHEN_PLAYER_OFF
                GROUP BY PLAYER_ID, GAME_DATE, SEASON;



CREATE TEMP TABLE AVG_PLAYER_WHEN_PLAYER_ON_OFF_DIFF AS
        SELECT AVG_ON_MIN - AVG_OFF_MIN AS AVG_ON_OFF_DIFF_MIN,
                AVG_ON_FGM - AVG_OFF_FGM AS AVG_ON_OFF_DIFF_FGM,
                AVG_ON_FGA - AVG_OFF_FGA AS AVG_ON_OFF_DIFF_FGA,
                AVG_ON_FG_PCT - AVG_OFF_FG_PCT AS AVG_ON_OFF_DIFF_FG_PCT,
                AVG_ON_FG3M - AVG_OFF_FG3M AS AVG_ON_OFF_DIFF_FG3M,
                AVG_ON_FG3A - AVG_OFF_FG3A AS AVG_ON_OFF_DIFF_FG3A,
                AVG_ON_FG3_PCT - AVG_OFF_FG3_PCT AS AVG_ON_OFF_DIFF_FG3_PCT,
                AVG_ON_FTM - AVG_OFF_FTM AS AVG_ON_OFF_DIFF_FTM,
                AVG_ON_FTA - AVG_OFF_FTA AS AVG_ON_OFF_DIFF_FTA,
                AVG_ON_FT_PCT - AVG_OFF_FT_PCT AS AVG_ON_OFF_DIFF_FT_PCT,
                AVG_ON_OREB - AVG_OFF_OREB AS AVG_ON_OFF_DIFF_OREB,
                AVG_ON_DREB - AVG_OFF_DREB AS AVG_ON_OFF_DIFF_DREB,
                AVG_ON_REB - AVG_OFF_REB AS AVG_ON_OFF_DIFF_REB,
                AVG_ON_AST - AVG_OFF_AST AS AVG_ON_OFF_DIFF_AST,
                AVG_ON_TOV - AVG_OFF_TOV AS AVG_ON_OFF_DIFF_TOV,
                AVG_ON_STL - AVG_OFF_STL AS AVG_ON_OFF_DIFF_STL,
                AVG_ON_BLK - AVG_OFF_BLK AS AVG_ON_OFF_DIFF_BLK,
                AVG_ON_BLKA - AVG_OFF_BLKA AS AVG_ON_OFF_DIFF_BLKA,
                AVG_ON_PF - AVG_OFF_PF AS AVG_ON_OFF_DIFF_PF,
                AVG_ON_PFD - AVG_OFF_PFD AS AVG_ON_OFF_DIFF_PFD,
                AVG_ON_PTS - AVG_OFF_PTS AS AVG_ON_OFF_DIFF_PTS,
                AVG_ON_PLUS_MINUS - AVG_OFF_PLUS_MINUS AS AVG_ON_OFF_DIFF_PLUS_MINUS,
                AVG_ON_NBA_FANTASY_PTS - AVG_OFF_NBA_FANTASY_PTS AS AVG_ON_OFF_DIFF_NBA_FANTASY_PTS,
                AVG_ON_DD2 - AVG_OFF_DD2 AS AVG_ON_OFF_DIFF_DD2,
                AVG_ON_TD3 - AVG_OFF_TD3 AS AVG_ON_OFF_DIFF_TD3,
                AVG_ON_OFF_RATING - AVG_OFF_OFF_RATING AS AVG_ON_OFF_DIFF_OFF_RATING,
                AVG_ON_DEF_RATING - AVG_OFF_DEF_RATING AS AVG_ON_OFF_DIFF_DEF_RATING,
                AVG_ON_NET_RATING - AVG_OFF_NET_RATING AS AVG_ON_OFF_DIFF_NET_RATING,
                AVG_ON_AST_PCT - AVG_OFF_AST_PCT AS AVG_ON_OFF_DIFF_AST_PCT,
                AVG_ON_AST_TO - AVG_OFF_AST_TO AS AVG_ON_OFF_DIFF_AST_TO,
                AVG_ON_AST_RATIO - AVG_OFF_AST_RATIO AS AVG_ON_OFF_DIFF_AST_RATIO,
                AVG_ON_OREB_PCT - AVG_OFF_OREB_PCT AS AVG_ON_OFF_DIFF_OREB_PCT,
                AVG_ON_DREB_PCT - AVG_OFF_DREB_PCT AS AVG_ON_OFF_DIFF_DREB_PCT,
                AVG_ON_REB_PCT - AVG_OFF_REB_PCT AS AVG_ON_OFF_DIFF_REB_PCT,
                AVG_ON_TM_TOV_PCT - AVG_OFF_TM_TOV_PCT AS AVG_ON_OFF_DIFF_TM_TOV_PCT,
                AVG_ON_EFG_PCT - AVG_OFF_EFG_PCT AS AVG_ON_OFF_DIFF_EFG_PCT,
                AVG_ON_TS_PCT - AVG_OFF_TS_PCT AS AVG_ON_OFF_DIFF_TS_PCT,
                AVG_ON_USG_PCT - AVG_OFF_USG_PCT AS AVG_ON_OFF_DIFF_USG_PCT,
                AVG_ON_PACE - AVG_OFF_PACE AS AVG_ON_OFF_DIFF_PACE,
                AVG_ON_PIE - AVG_OFF_PIE AS AVG_ON_OFF_DIFF_PIE,
                AVG_ON_FGM_PG - AVG_OFF_FGM_PG AS AVG_ON_OFF_DIFF_FGM_PG,
                AVG_ON_FGA_PG - AVG_OFF_FGA_PG AS AVG_ON_OFF_DIFF_FGA_PG,

                AVG_PLAYER_WHEN_PLAYER_ON.PLAYER_ID,
                AVG_PLAYER_WHEN_PLAYER_ON.GAME_DATE,
                AVG_PLAYER_WHEN_PLAYER_ON.SEASON
            FROM AVG_PLAYER_WHEN_PLAYER_ON
                INNER JOIN AVG_PLAYER_WHEN_PLAYER_OFF
                    ON AVG_PLAYER_WHEN_PLAYER_ON.PLAYER_ID = AVG_PLAYER_WHEN_PLAYER_OFF.PLAYER_ID
                        AND AVG_PLAYER_WHEN_PLAYER_ON.GAME_DATE = AVG_PLAYER_WHEN_PLAYER_OFF.GAME_DATE
                        AND AVG_PLAYER_WHEN_PLAYER_ON.SEASON = AVG_PLAYER_WHEN_PLAYER_OFF.SEASON
                        AND AVG_PLAYER_WHEN_PLAYER_ON.INJURED_PLAYER_ID = AVG_PLAYER_WHEN_PLAYER_OFF.INJURED_PLAYER_ID;
DROP TABLE AVG_PLAYER_WHEN_PLAYER_OFF;
DROP TABLE AVG_PLAYER_WHEN_PLAYER_ON;

CREATE TEMP TABLE AVG_PLAYER_ON_OFF_DIFF AS
        SELECT AVG(AVG_ON_OFF_DIFF_MIN) AS AVG_AVG_ON_OFF_DIFF_MIN,
                AVG(AVG_ON_OFF_DIFF_FGM) AS AVG_AVG_ON_OFF_DIFF_FGM,
                AVG(AVG_ON_OFF_DIFF_FGA) AS AVG_AVG_ON_OFF_DIFF_FGA,
                AVG(AVG_ON_OFF_DIFF_FG_PCT) AS AVG_AVG_ON_OFF_DIFF_FG_PCT,
                AVG(AVG_ON_OFF_DIFF_FG3M) AS AVG_AVG_ON_OFF_DIFF_FG3M,
                AVG(AVG_ON_OFF_DIFF_FG3A) AS AVG_AVG_ON_OFF_DIFF_FG3A,
                AVG(AVG_ON_OFF_DIFF_FG3_PCT) AS AVG_AVG_ON_OFF_DIFF_FG3_PCT,
                AVG(AVG_ON_OFF_DIFF_FTM) AS AVG_AVG_ON_OFF_DIFF_FTM,
                AVG(AVG_ON_OFF_DIFF_FTA) AS AVG_AVG_ON_OFF_DIFF_FTA,
                AVG(AVG_ON_OFF_DIFF_FT_PCT) AS AVG_AVG_ON_OFF_DIFF_FT_PCT,
                AVG(AVG_ON_OFF_DIFF_OREB) AS AVG_AVG_ON_OFF_DIFF_OREB,
                AVG(AVG_ON_OFF_DIFF_DREB) AS AVG_AVG_ON_OFF_DIFF_DREB,
                AVG(AVG_ON_OFF_DIFF_REB) AS AVG_AVG_ON_OFF_DIFF_REB,
                AVG(AVG_ON_OFF_DIFF_AST) AS AVG_AVG_ON_OFF_DIFF_AST,
                AVG(AVG_ON_OFF_DIFF_TOV) AS AVG_AVG_ON_OFF_DIFF_TOV,
                AVG(AVG_ON_OFF_DIFF_STL) AS AVG_AVG_ON_OFF_DIFF_STL,
                AVG(AVG_ON_OFF_DIFF_BLK) AS AVG_AVG_ON_OFF_DIFF_BLK,
                AVG(AVG_ON_OFF_DIFF_BLKA) AS AVG_AVG_ON_OFF_DIFF_BLKA,
                AVG(AVG_ON_OFF_DIFF_PF) AS AVG_AVG_ON_OFF_DIFF_PF,
                AVG(AVG_ON_OFF_DIFF_PFD) AS AVG_AVG_ON_OFF_DIFF_PFD,
                AVG(AVG_ON_OFF_DIFF_PTS) AS AVG_AVG_ON_OFF_DIFF_PTS,
                AVG(AVG_ON_OFF_DIFF_PLUS_MINUS) AS AVG_AVG_ON_OFF_DIFF_PLUS_MINUS,
                AVG(AVG_ON_OFF_DIFF_NBA_FANTASY_PTS) AS AVG_AVG_ON_OFF_DIFF_NBA_FANTASY_PTS,
                AVG(AVG_ON_OFF_DIFF_DD2) AS AVG_AVG_ON_OFF_DIFF_DD2,
                AVG(AVG_ON_OFF_DIFF_TD3) AS AVG_AVG_ON_OFF_DIFF_TD3,

                PLAYER_ID, GAME_DATE, SEASON
            FROM AVG_PLAYER_WHEN_PLAYER_ON_OFF_DIFF
                GROUP BY PLAYER_ID, GAME_DATE, SEASON;

CREATE TEMP TABLE MAX_PLAYER_ON_OFF_DIFF AS
        SELECT MAX(AVG_ON_OFF_DIFF_MIN) AS MAX_AVG_ON_OFF_DIFF_MIN,
                MAX(AVG_ON_OFF_DIFF_FGM) AS MAX_AVG_ON_OFF_DIFF_FGM,
                MAX(AVG_ON_OFF_DIFF_FGA) AS MAX_AVG_ON_OFF_DIFF_FGA,
                MAX(AVG_ON_OFF_DIFF_FG_PCT) AS MAX_AVG_ON_OFF_DIFF_FG_PCT,
                MAX(AVG_ON_OFF_DIFF_FG3M) AS MAX_AVG_ON_OFF_DIFF_FG3M,
                MAX(AVG_ON_OFF_DIFF_FG3A) AS MAX_AVG_ON_OFF_DIFF_FG3A,
                MAX(AVG_ON_OFF_DIFF_FG3_PCT) AS MAX_AVG_ON_OFF_DIFF_FG3_PCT,
                MAX(AVG_ON_OFF_DIFF_FTM) AS MAX_AVG_ON_OFF_DIFF_FTM,
                MAX(AVG_ON_OFF_DIFF_FTA) AS MAX_AVG_ON_OFF_DIFF_FTA,
                MAX(AVG_ON_OFF_DIFF_FT_PCT) AS MAX_AVG_ON_OFF_DIFF_FT_PCT,
                MAX(AVG_ON_OFF_DIFF_OREB) AS MAX_AVG_ON_OFF_DIFF_OREB,
                MAX(AVG_ON_OFF_DIFF_DREB) AS MAX_AVG_ON_OFF_DIFF_DREB,
                MAX(AVG_ON_OFF_DIFF_REB) AS MAX_AVG_ON_OFF_DIFF_REB,
                MAX(AVG_ON_OFF_DIFF_AST) AS MAX_AVG_ON_OFF_DIFF_AST,
                MAX(AVG_ON_OFF_DIFF_TOV) AS MAX_AVG_ON_OFF_DIFF_TOV,
                MAX(AVG_ON_OFF_DIFF_STL) AS MAX_AVG_ON_OFF_DIFF_STL,
                MAX(AVG_ON_OFF_DIFF_BLK) AS MAX_AVG_ON_OFF_DIFF_BLK,
                MAX(AVG_ON_OFF_DIFF_BLKA) AS MAX_AVG_ON_OFF_DIFF_BLKA,
                MAX(AVG_ON_OFF_DIFF_PF) AS MAX_AVG_ON_OFF_DIFF_PF,
                MAX(AVG_ON_OFF_DIFF_PFD) AS MAX_AVG_ON_OFF_DIFF_PFD,
                MAX(AVG_ON_OFF_DIFF_PTS) AS MAX_AVG_ON_OFF_DIFF_PTS,
                MAX(AVG_ON_OFF_DIFF_PLUS_MINUS) AS MAX_AVG_ON_OFF_DIFF_PLUS_MINUS,
                MAX(AVG_ON_OFF_DIFF_NBA_FANTASY_PTS) AS MAX_AVG_ON_OFF_DIFF_NBA_FANTASY_PTS,
                MAX(AVG_ON_OFF_DIFF_DD2) AS MAX_AVG_ON_OFF_DIFF_DD2,
                MAX(AVG_ON_OFF_DIFF_TD3) AS MAX_AVG_ON_OFF_DIFF_TD3,

                PLAYER_ID, GAME_DATE, SEASON
            FROM AVG_PLAYER_WHEN_PLAYER_ON_OFF_DIFF
                GROUP BY PLAYER_ID, GAME_DATE, SEASON;

CREATE TEMP TABLE MIN_PLAYER_ON_OFF_DIFF AS
        SELECT MIN(AVG_ON_OFF_DIFF_MIN) AS MIN_AVG_ON_OFF_DIFF_MIN,
                MIN(AVG_ON_OFF_DIFF_FGM) AS MIN_AVG_ON_OFF_DIFF_FGM,
                MIN(AVG_ON_OFF_DIFF_FGA) AS MIN_AVG_ON_OFF_DIFF_FGA,
                MIN(AVG_ON_OFF_DIFF_FG_PCT) AS MIN_AVG_ON_OFF_DIFF_FG_PCT,
                MIN(AVG_ON_OFF_DIFF_FG3M) AS MIN_AVG_ON_OFF_DIFF_FG3M,
                MIN(AVG_ON_OFF_DIFF_FG3A) AS MIN_AVG_ON_OFF_DIFF_FG3A,
                MIN(AVG_ON_OFF_DIFF_FG3_PCT) AS MIN_AVG_ON_OFF_DIFF_FG3_PCT,
                MIN(AVG_ON_OFF_DIFF_FTM) AS MIN_AVG_ON_OFF_DIFF_FTM,
                MIN(AVG_ON_OFF_DIFF_FTA) AS MIN_AVG_ON_OFF_DIFF_FTA,
                MIN(AVG_ON_OFF_DIFF_FT_PCT) AS MIN_AVG_ON_OFF_DIFF_FT_PCT,
                MIN(AVG_ON_OFF_DIFF_OREB) AS MIN_AVG_ON_OFF_DIFF_OREB,
                MIN(AVG_ON_OFF_DIFF_DREB) AS MIN_AVG_ON_OFF_DIFF_DREB,
                MIN(AVG_ON_OFF_DIFF_REB) AS MIN_AVG_ON_OFF_DIFF_REB,
                MIN(AVG_ON_OFF_DIFF_AST) AS MIN_AVG_ON_OFF_DIFF_AST,
                MIN(AVG_ON_OFF_DIFF_TOV) AS MIN_AVG_ON_OFF_DIFF_TOV,
                MIN(AVG_ON_OFF_DIFF_STL) AS MIN_AVG_ON_OFF_DIFF_STL,
                MIN(AVG_ON_OFF_DIFF_BLK) AS MIN_AVG_ON_OFF_DIFF_BLK,
                MIN(AVG_ON_OFF_DIFF_BLKA) AS MIN_AVG_ON_OFF_DIFF_BLKA,
                MIN(AVG_ON_OFF_DIFF_PF) AS MIN_AVG_ON_OFF_DIFF_PF,
                MIN(AVG_ON_OFF_DIFF_PFD) AS MIN_AVG_ON_OFF_DIFF_PFD,
                MIN(AVG_ON_OFF_DIFF_PTS) AS MIN_AVG_ON_OFF_DIFF_PTS,
                MIN(AVG_ON_OFF_DIFF_PLUS_MINUS) AS MIN_AVG_ON_OFF_DIFF_PLUS_MINUS,
                MIN(AVG_ON_OFF_DIFF_NBA_FANTASY_PTS) AS MIN_AVG_ON_OFF_DIFF_NBA_FANTASY_PTS,
                MIN(AVG_ON_OFF_DIFF_DD2) AS MIN_AVG_ON_OFF_DIFF_DD2,
                MIN(AVG_ON_OFF_DIFF_TD3) AS MIN_AVG_ON_OFF_DIFF_TD3,

                PLAYER_ID, GAME_DATE, SEASON
            FROM AVG_PLAYER_WHEN_PLAYER_ON_OFF_DIFF
                GROUP BY PLAYER_ID, GAME_DATE, SEASON;

CREATE TEMP TABLE SUM_PLAYER_ON_OFF_DIFF AS
        SELECT SUM(AVG_ON_OFF_DIFF_MIN) AS SUM_AVG_ON_OFF_DIFF_MIN,
                SUM(AVG_ON_OFF_DIFF_FGM) AS SUM_AVG_ON_OFF_DIFF_FGM,
                SUM(AVG_ON_OFF_DIFF_FGA) AS SUM_AVG_ON_OFF_DIFF_FGA,
                SUM(AVG_ON_OFF_DIFF_FG_PCT) AS SUM_AVG_ON_OFF_DIFF_FG_PCT,
                SUM(AVG_ON_OFF_DIFF_FG3M) AS SUM_AVG_ON_OFF_DIFF_FG3M,
                SUM(AVG_ON_OFF_DIFF_FG3A) AS SUM_AVG_ON_OFF_DIFF_FG3A,
                SUM(AVG_ON_OFF_DIFF_FG3_PCT) AS SUM_AVG_ON_OFF_DIFF_FG3_PCT,
                SUM(AVG_ON_OFF_DIFF_FTM) AS SUM_AVG_ON_OFF_DIFF_FTM,
                SUM(AVG_ON_OFF_DIFF_FTA) AS SUM_AVG_ON_OFF_DIFF_FTA,
                SUM(AVG_ON_OFF_DIFF_FT_PCT) AS SUM_AVG_ON_OFF_DIFF_FT_PCT,
                SUM(AVG_ON_OFF_DIFF_OREB) AS SUM_AVG_ON_OFF_DIFF_OREB,
                SUM(AVG_ON_OFF_DIFF_DREB) AS SUM_AVG_ON_OFF_DIFF_DREB,
                SUM(AVG_ON_OFF_DIFF_REB) AS SUM_AVG_ON_OFF_DIFF_REB,
                SUM(AVG_ON_OFF_DIFF_AST) AS SUM_AVG_ON_OFF_DIFF_AST,
                SUM(AVG_ON_OFF_DIFF_TOV) AS SUM_AVG_ON_OFF_DIFF_TOV,
                SUM(AVG_ON_OFF_DIFF_STL) AS SUM_AVG_ON_OFF_DIFF_STL,
                SUM(AVG_ON_OFF_DIFF_BLK) AS SUM_AVG_ON_OFF_DIFF_BLK,
                SUM(AVG_ON_OFF_DIFF_BLKA) AS SUM_AVG_ON_OFF_DIFF_BLKA,
                SUM(AVG_ON_OFF_DIFF_PF) AS SUM_AVG_ON_OFF_DIFF_PF,
                SUM(AVG_ON_OFF_DIFF_PFD) AS SUM_AVG_ON_OFF_DIFF_PFD,
                SUM(AVG_ON_OFF_DIFF_PTS) AS SUM_AVG_ON_OFF_DIFF_PTS,
                SUM(AVG_ON_OFF_DIFF_PLUS_MINUS) AS SUM_AVG_ON_OFF_DIFF_PLUS_MINUS,
                SUM(AVG_ON_OFF_DIFF_NBA_FANTASY_PTS) AS SUM_AVG_ON_OFF_DIFF_NBA_FANTASY_PTS,
                SUM(AVG_ON_OFF_DIFF_DD2) AS SUM_AVG_ON_OFF_DIFF_DD2,
                SUM(AVG_ON_OFF_DIFF_TD3) AS SUM_AVG_ON_OFF_DIFF_TD3,

                PLAYER_ID, GAME_DATE, SEASON
            FROM AVG_PLAYER_WHEN_PLAYER_ON_OFF_DIFF
                GROUP BY PLAYER_ID, GAME_DATE, SEASON;

DROP TABLE AVG_PLAYER_WHEN_PLAYER_ON_OFF_DIFF;