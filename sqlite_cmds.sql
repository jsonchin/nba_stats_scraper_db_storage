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

CREATE TEMP TABLE POS_G AS
    SELECT PLAYER_ID, SEASON FROM PLAYER_POSITIONS WHERE PLAYER_POSITION = "G";

CREATE TEMP TABLE POS_F AS
    SELECT PLAYER_ID, SEASON FROM PLAYER_POSITIONS WHERE PLAYER_POSITION = "F";

CREATE TEMP TABLE POS_C AS
    SELECT PLAYER_ID, SEASON FROM PLAYER_POSITIONS WHERE PLAYER_POSITION = "C";

CREATE TEMP TABLE POS_GF AS
    SELECT * FROM POS_G UNION SELECT * FROM POS_F;

CREATE TEMP TABLE POS_FC AS
    SELECT * FROM POS_F UNION SELECT * FROM POS_C;

CREATE TEMP TABLE POS_TO_PLAYER_IDS AS
    SELECT 'G' AS PLAYER_POSITION, * FROM POS_G
    UNION SELECT 'F' AS PLAYER_POSITION, * FROM POS_F
    UNION SELECT 'C' AS PLAYER_POSITION, * FROM POS_C
    UNION SELECT 'FG' AS PLAYER_POSITION, * FROM POS_GF
    UNION SELECT 'CF' AS PLAYER_POSITION, * FROM POS_FC;

DROP TABLE IF EXISTS PLAYER_IDS_TO_MAX_POS;
CREATE TABLE PLAYER_IDS_TO_MAX_POS AS
    SELECT PLAYER_ID, SEASON, PLAYER_POSITION
        FROM POS_TO_PLAYER_IDS
        GROUP BY PLAYER_ID, SEASON
        ORDER BY LENGTH(PLAYER_POSITION) DESC;

CREATE TEMP TABLE VALID_PLAYER_POSITIONS (PLAYER_POSITION TEXT);
INSERT INTO VALID_PLAYER_POSITIONS (PLAYER_POSITION) VALUES ('G');
INSERT INTO VALID_PLAYER_POSITIONS (PLAYER_POSITION) VALUES ('F');
INSERT INTO VALID_PLAYER_POSITIONS (PLAYER_POSITION) VALUES ('C');
INSERT INTO VALID_PLAYER_POSITIONS (PLAYER_POSITION) VALUES ('FG');
INSERT INTO VALID_PLAYER_POSITIONS (PLAYER_POSITION) VALUES ('CF');

DROP TABLE IF EXISTS dnp_stats_by_position;
CREATE TABLE dnp_stats_by_position AS
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
        COALESCE(SUM(p_avg_stats_dnp.DD2), 0) AS TOTAL_DNP_DD2,
        COALESCE(SUM(p_avg_stats_dnp.TD3), 0) AS TOTAL_DNP_TD3,
        COALESCE(SUM(p_avg_stats_dnp.DD2) * 1.0  / SUM(p_avg_stats_dnp.GP), 0) AS DNP_DD2,
        COALESCE(SUM(p_avg_stats_dnp.TD3) * 1.0  / SUM(p_avg_stats_dnp.GP), 0) AS DNP_TD3,
        dnp.GAME_ID AS GAME_ID,
        dnp.TEAM_ID AS TEAM_ID,
        dnp.SEASON AS SEASON,
        valid_player_positions.PLAYER_POSITION AS PLAYER_POSITION
        FROM DID_NOT_PLAY AS dnp
            INNER JOIN VALID_PLAYER_POSITIONS AS valid_player_positions
                ON 1 = 1
            LEFT JOIN POS_TO_PLAYER_IDS AS pos_to_player_ids
                ON pos_to_player_ids.PLAYER_POSITION = valid_player_positions.PLAYER_POSITION
                    AND pos_to_player_ids.SEASON = dnp.SEASON
                    AND pos_to_player_ids.PLAYER_ID = dnp.PLAYER_ID
            LEFT JOIN GENERAL_TRADITIONAL_PLAYER_STATS AS p_avg_stats_dnp
                ON pos_to_player_ids.PLAYER_ID = p_avg_stats_dnp.PLAYER_ID
                    AND dnp.SEASON = p_avg_stats_dnp.SEASON
                    AND DATE(dnp.GAME_DATE, '-1 day') = p_avg_stats_dnp.DATE_TO
        GROUP BY
            dnp.SEASON,
            dnp.GAME_ID,
            dnp.TEAM_ID,
            valid_player_positions.PLAYER_POSITION;

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
        COALESCE(SUM(p_avg_stats_dnp.DD2), 0) AS TOTAL_DNP_DD2,
        COALESCE(SUM(p_avg_stats_dnp.TD3), 0) AS TOTAL_DNP_TD3,
        COALESCE(SUM(p_avg_stats_dnp.DD2) * 1.0  / SUM(p_avg_stats_dnp.GP), 0) AS DNP_DD2,
        COALESCE(SUM(p_avg_stats_dnp.TD3) * 1.0  / SUM(p_avg_stats_dnp.GP), 0) AS DNP_TD3,
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



CREATE TEMP TABLE PLAYER_INJURED_PLAYER AS
    SELECT p_log.*, dnp.PLAYER_ID AS INJURED_PLAYER_ID FROM PLAYER_LOGS AS p_log
        INNER JOIN DID_NOT_PLAY AS dnp
            ON p_log.GAME_ID = dnp.GAME_ID
                AND p_log.TEAM_ID = dnp.TEAM_ID
                AND dnp.PLAYER_ID IS NOT NULL;

CREATE TEMP TABLE AVG_PLAYER_WHEN_PLAYER_INJURED AS
    SELECT AVG(player_injured_pairs.MIN) AS AVG_INJURED_MIN,
            AVG(player_injured_pairs.FGM) AS AVG_INJURED_FGM,
            AVG(player_injured_pairs.FGA) AS AVG_INJURED_FGA,
            AVG(player_injured_pairs.FG_PCT) AS AVG_INJURED_FG_PCT,
            AVG(player_injured_pairs.FG3M) AS AVG_INJURED_FG3M,
            AVG(player_injured_pairs.FG3A) AS AVG_INJURED_FG3A,
            AVG(player_injured_pairs.FG3_PCT) AS AVG_INJURED_FG3_PCT,
            AVG(player_injured_pairs.FTM) AS AVG_INJURED_FTM,
            AVG(player_injured_pairs.FTA) AS AVG_INJURED_FTA,
            AVG(player_injured_pairs.FT_PCT) AS AVG_INJURED_FT_PCT,
            AVG(player_injured_pairs.OREB) AS AVG_INJURED_OREB,
            AVG(player_injured_pairs.DREB) AS AVG_INJURED_DREB,
            AVG(player_injured_pairs.REB) AS AVG_INJURED_REB,
            AVG(player_injured_pairs.AST) AS AVG_INJURED_AST,
            AVG(player_injured_pairs.TOV) AS AVG_INJURED_TOV,
            AVG(player_injured_pairs.STL) AS AVG_INJURED_STL,
            AVG(player_injured_pairs.BLK) AS AVG_INJURED_BLK,
            AVG(player_injured_pairs.BLKA) AS AVG_INJURED_BLKA,
            AVG(player_injured_pairs.PF) AS AVG_INJURED_PF,
            AVG(player_injured_pairs.PFD) AS AVG_INJURED_PFD,
            AVG(player_injured_pairs.PTS) AS AVG_INJURED_PTS,
            AVG(player_injured_pairs.PLUS_MINUS) AS AVG_INJURED_PLUS_MINUS,
            AVG(player_injured_pairs.NBA_FANTASY_PTS) AS AVG_INJURED_NBA_FANTASY_PTS,
            AVG(player_injured_pairs.DD2) AS AVG_INJURED_DD2,
            AVG(player_injured_pairs.TD3) AS AVG_INJURED_TD3,
            COUNT(*) AS NUM_GAMES_PLAYED_WITH_INJURED,
            p_log.PLAYER_ID AS PLAYER_ID,
            player_injured_pairs.INJURED_PLAYER_ID AS INJURED_PLAYER_ID,
            p_log.GAME_DATE AS GAME_DATE,
            p_log.SEASON AS SEASON

    FROM PLAYER_LOGS AS p_log
        INNER JOIN PLAYER_INJURED_PLAYER AS player_injured_pairs
            ON p_log.SEASON = player_injured_pairs.SEASON
                AND p_log.PLAYER_ID = player_injured_pairs.PLAYER_ID
                AND p_log.GAME_DATE > player_injured_pairs.GAME_DATE
        GROUP BY p_log.PLAYER_ID, player_injured_pairs.INJURED_PLAYER_ID,
                p_log.SEASON, p_log.GAME_DATE;

CREATE TEMP TABLE AVG_PLAYER_INJURED AS
    SELECT AVG(AVG_INJURED_MIN) AS AVG_INJURED_MIN,
        AVG(AVG_INJURED_FGM) AS AVG_INJURED_FGM,
        AVG(AVG_INJURED_FGA) AS AVG_INJURED_FGA,
        AVG(AVG_INJURED_FG_PCT) AS AVG_INJURED_FG_PCT,
        AVG(AVG_INJURED_FG3M) AS AVG_INJURED_FG3M,
        AVG(AVG_INJURED_FG3A) AS AVG_INJURED_FG3A,
        AVG(AVG_INJURED_FG3_PCT) AS AVG_INJURED_FG3_PCT,
        AVG(AVG_INJURED_FTM) AS AVG_INJURED_FTM,
        AVG(AVG_INJURED_FTA) AS AVG_INJURED_FTA,
        AVG(AVG_INJURED_FT_PCT) AS AVG_INJURED_FT_PCT,
        AVG(AVG_INJURED_OREB) AS AVG_INJURED_OREB,
        AVG(AVG_INJURED_DREB) AS AVG_INJURED_DREB,
        AVG(AVG_INJURED_REB) AS AVG_INJURED_REB,
        AVG(AVG_INJURED_AST) AS AVG_INJURED_AST,
        AVG(AVG_INJURED_TOV) AS AVG_INJURED_TOV,
        AVG(AVG_INJURED_STL) AS AVG_INJURED_STL,
        AVG(AVG_INJURED_BLK) AS AVG_INJURED_BLK,
        AVG(AVG_INJURED_BLKA) AS AVG_INJURED_BLKA,
        AVG(AVG_INJURED_PF) AS AVG_INJURED_PF,
        AVG(AVG_INJURED_PFD) AS AVG_INJURED_PFD,
        AVG(AVG_INJURED_PTS) AS AVG_INJURED_PTS,
        AVG(AVG_INJURED_PLUS_MINUS) AS AVG_INJURED_PLUS_MINUS,
        AVG(AVG_INJURED_NBA_FANTASY_PTS) AS AVG_INJURED_NBA_FANTASY_PTS,
        AVG(AVG_INJURED_DD2) AS AVG_INJURED_DD2,
        AVG(AVG_INJURED_TD3) AS AVG_INJURED_TD3,
        PLAYER_ID, GAME_DATE, SEASON
    FROM AVG_PLAYER_WHEN_PLAYER_INJURED
        GROUP BY PLAYER_ID, GAME_DATE, SEASON;

CREATE TEMP TABLE MAX_PLAYER_INJURIES AS
    SELECT
        MAX(AVG_INJURED_MIN) AS MAX_INJURED_MIN,
        MAX(AVG_INJURED_FGM) AS MAX_INJURED_FGM,
        MAX(AVG_INJURED_FGA) AS MAX_INJURED_FGA,
        MAX(AVG_INJURED_FG_PCT) AS MAX_INJURED_FG_PCT,
        MAX(AVG_INJURED_FG3M) AS MAX_INJURED_FG3M,
        MAX(AVG_INJURED_FG3A) AS MAX_INJURED_FG3A,
        MAX(AVG_INJURED_FG3_PCT) AS MAX_INJURED_FG3_PCT,
        MAX(AVG_INJURED_FTM) AS MAX_INJURED_FTM,
        MAX(AVG_INJURED_FTA) AS MAX_INJURED_FTA,
        MAX(AVG_INJURED_FT_PCT) AS MAX_INJURED_FT_PCT,
        MAX(AVG_INJURED_OREB) AS MAX_INJURED_OREB,
        MAX(AVG_INJURED_DREB) AS MAX_INJURED_DREB,
        MAX(AVG_INJURED_REB) AS MAX_INJURED_REB,
        MAX(AVG_INJURED_AST) AS MAX_INJURED_AST,
        MAX(AVG_INJURED_TOV) AS MAX_INJURED_TOV,
        MAX(AVG_INJURED_STL) AS MAX_INJURED_STL,
        MAX(AVG_INJURED_BLK) AS MAX_INJURED_BLK,
        MAX(AVG_INJURED_BLKA) AS MAX_INJURED_BLKA,
        MAX(AVG_INJURED_PF) AS MAX_INJURED_PF,
        MAX(AVG_INJURED_PFD) AS MAX_INJURED_PFD,
        MAX(AVG_INJURED_PTS) AS MAX_INJURED_PTS,
        MAX(AVG_INJURED_PLUS_MINUS) AS MAX_INJURED_PLUS_MINUS,
        MAX(AVG_INJURED_NBA_FANTASY_PTS) AS MAX_INJURED_NBA_FANTASY_PTS,
        MAX(AVG_INJURED_DD2) AS MAX_INJURED_DD2,
        MAX(AVG_INJURED_TD3) AS MAX_INJURED_TD3,
        PLAYER_ID, GAME_DATE, SEASON

    FROM AVG_PLAYER_WHEN_PLAYER_INJURED
        GROUP BY PLAYER_ID, GAME_DATE, SEASON;