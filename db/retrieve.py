"""
Defines ease of use retrieval functions such as for fetching
all player_ids which occurs often, as well as a general function
for querying with standard SQL.

The general query function will return a pandas dataframe.
"""
import db.utils
from collections import defaultdict
import pandas as pd
import os
from scrape.config import START_YEAR


def fetch_player_ids():
    """
    Returns a mapping of season to a list of player_ids.
    """
    player_ids_by_season = defaultdict(list)
    season_player_id_tuples = db.utils.execute_sql("""SELECT SEASON, PLAYER_ID FROM player_ids;""").rows

    for season, player_id in season_player_id_tuples:
        player_ids_by_season[season].append(player_id)

    return player_ids_by_season


def fetch_game_dates():
    """
    Returns a mapping of season to a list of game dates.
    """
    game_dates_by_season = defaultdict(list)
    season_game_date_tuples = db.utils.execute_sql("""SELECT SEASON, GAME_DATE FROM game_dates;""").rows

    for season, game_date in season_game_date_tuples:
        game_dates_by_season[season].append(game_date)

    return game_dates_by_season


def fetch_game_ids():
    """
    Returns a mapping of season to a list of game ids.
    """
    game_ids_by_season = defaultdict(list)
    season_game_id_tuples = db.utils.execute_sql("""SELECT SEASON, GAME_ID FROM games GROUP BY SEASON, GAME_ID;""").rows

    for season, game_id in season_game_id_tuples:
        game_ids_by_season[season].append(game_id)

    return game_ids_by_season


def aggregate_data():
    """
    Aggregates all of the data in the database into rows by
    GAME_DATE, SEASON, and PLAYER_ID
    """
    # TODO implement dynamically


def aggregate_daily_data(season='2017-18'):
    return db_query("""
        SELECT * FROM player_logs
            WHERE SEASON = ? AND GAME_DATE =
                (SELECT MAX(inner_player_logs.GAME_DATE)
                    FROM player_logs AS inner_player_logs
                    WHERE inner_player_logs.SEASON = ? AND inner_player_logs.PLAYER_ID = player_logs.PLAYER_ID
                    GROUP BY inner_player_logs.PLAYER_ID);""", (season, season))


def aggregate_training_data(con, filter_fp=-10, filter_season=START_YEAR):
    """
    Aggregates training data as defined by:
        FP, [previous_game's stats]
    and returns a Pandas DataFrame corresponding to the query.
    """

    return db_query("""
        SELECT ROUND(p_log_future.PTS
                + 1.2 * p_log_future.REB
                + 1.5 * p_log_future.AST
                + 3 * p_log_future.BLK
                + 3 * p_log_future.STL
                + -1 * p_log_future.TOV, 1) AS FP_TO_PREDICT,
                
                ROUND(p_log_future.PTS
                + 0.5 * p_log_future.FG3M
                + 1.25 * p_log_future.REB
                + 1.5 * p_log_future.AST
                + 2 * p_log_future.BLK
                + 2 * p_log_future.STL
                + -0.5 * p_log_future.TOV
                + 1.5 * p_log_future.DD2
                + 3 * p_log_future.TD3, 2) AS DK_FP_TO_PREDICT,
                
                p_log_future.PTS AS PTS_TO_PREDICT,
                p_log_future.REB AS REB_TO_PREDICT,
                p_log_future.AST AS AST_TO_PREDICT,
                p_log_future.BLK AS BLK_TO_PREDICT,
                p_log_future.STL AS STL_TO_PREDICT,
                p_log_future.TOV AS TOV_TO_PREDICT,
                
                p_log_future.MIN AS MIN_TO_PREDICT,
                
                 ROUND(p_log_today.PTS
                + 1.2 * p_log_today.REB
                + 1.5 * p_log_today.AST
                + 3 * p_log_today.BLK
                + 3 * p_log_today.STL
                + -1 * p_log_today.TOV, 1) AS FP,
                
                ROUND(p_log_today.PTS
                + 0.5 * p_log_today.FG3M
                + 1.25 * p_log_today.REB
                + 1.5 * p_log_today.AST
                + 2 * p_log_today.BLK
                + 2 * p_log_today.STL
                + -0.5 * p_log_today.TOV
                + 1.5 * p_log_today.DD2
                + 3 * p_log_today.TD3, 2) AS DK_FP,
                
                p_log_today.PLAYER_ID AS PLAYER_ID, p_log_today.PLAYER_NAME AS PLAYER_NAME, p_log_today.TEAM_ID AS TEAM_ID, p_log_today.TEAM_ABBREVIATION AS TEAM_ABBREVIATION, p_log_today.TEAM_NAME AS TEAM_NAME, p_log_today.GAME_ID AS GAME_ID, p_log_today.GAME_DATE AS GAME_DATE, p_log_today.MATCHUP AS MATCHUP, p_log_today.WL AS WL, p_log_today.MIN AS MIN, p_log_today.FGM AS FGM, p_log_today.FGA AS FGA, p_log_today.FG_PCT AS FG_PCT, p_log_today.FG3M AS FG3M, p_log_today.FG3A AS FG3A, p_log_today.FG3_PCT AS FG3_PCT, p_log_today.FTM AS FTM, p_log_today.FTA AS FTA, p_log_today.FT_PCT AS FT_PCT, p_log_today.OREB AS OREB, p_log_today.DREB AS DREB, p_log_today.REB AS REB, p_log_today.AST AS AST, p_log_today.TOV AS TOV, p_log_today.STL AS STL, p_log_today.BLK AS BLK, p_log_today.BLKA AS BLKA, p_log_today.PF AS PF, p_log_today.PFD AS PFD, p_log_today.PTS AS PTS, p_log_today.PLUS_MINUS AS PLUS_MINUS, p_log_today.NBA_FANTASY_PTS AS NBA_FANTASY_PTS, p_log_today.DD2 AS DD2, p_log_today.TD3 AS TD3, p_log_today.SEASON AS SEASON,
                
                p_avg_stats.W_PCT AS AVG_W_PCT, p_avg_stats.MIN AS AVG_MIN, p_avg_stats.FGM AS AVG_FGM, p_avg_stats.FGA AS AVG_FGA, p_avg_stats.FG_PCT AS AVG_FG_PCT, p_avg_stats.FG3M AS AVG_FG3M, p_avg_stats.FG3A AS AVG_FG3A, p_avg_stats.FG3_PCT AS AVG_FG3_PCT, p_avg_stats.FTM AS AVG_FTM, p_avg_stats.FTA AS AVG_FTA, p_avg_stats.FT_PCT AS AVG_FT_PCT, p_avg_stats.OREB AS AVG_OREB, p_avg_stats.DREB AS AVG_DREB, p_avg_stats.REB AS AVG_REB, p_avg_stats.AST AS AVG_AST, p_avg_stats.TOV AS AVG_TOV, p_avg_stats.STL AS AVG_STL, p_avg_stats.BLK AS AVG_BLK, p_avg_stats.BLKA AS AVG_BLKA, p_avg_stats.PF AS AVG_PF, p_avg_stats.PFD AS AVG_PFD, p_avg_stats.PTS AS AVG_PTS, p_avg_stats.PLUS_MINUS AS AVG_PLUS_MINUS, p_avg_stats.NBA_FANTASY_PTS AS AVG_NBA_FANTASY_PTS,
                p_avg_stats.DD2 * 1.0 /p_avg_stats.GP AS AVG_DD2, p_avg_stats.TD3 * 1.0 /p_avg_stats.GP AS AVG_TD3,

                p_avg_pace_stats.W_PCT AS AVG_PACE_W_PCT, p_avg_pace_stats.MIN AS AVG_PACE_MIN, p_avg_pace_stats.FGM AS AVG_PACE_FGM, p_avg_pace_stats.FGA AS AVG_PACE_FGA, p_avg_pace_stats.FG_PCT AS AVG_PACE_FG_PCT, p_avg_pace_stats.FG3M AS AVG_PACE_FG3M, p_avg_pace_stats.FG3A AS AVG_PACE_FG3A, p_avg_pace_stats.FG3_PCT AS AVG_PACE_FG3_PCT, p_avg_pace_stats.FTM AS AVG_PACE_FTM, p_avg_pace_stats.FTA AS AVG_PACE_FTA, p_avg_pace_stats.FT_PCT AS AVG_PACE_FT_PCT, p_avg_pace_stats.OREB AS AVG_PACE_OREB, p_avg_pace_stats.DREB AS AVG_PACE_DREB, p_avg_pace_stats.REB AS AVG_PACE_REB, p_avg_pace_stats.AST AS AVG_PACE_AST, p_avg_pace_stats.TOV AS AVG_PACE_TOV, p_avg_pace_stats.STL AS AVG_PACE_STL, p_avg_pace_stats.BLK AS AVG_PACE_BLK, p_avg_pace_stats.BLKA AS AVG_PACE_BLKA, p_avg_pace_stats.PF AS AVG_PACE_PF, p_avg_pace_stats.PFD AS AVG_PACE_PFD, p_avg_pace_stats.PTS AS AVG_PACE_PTS, p_avg_pace_stats.PLUS_MINUS AS AVG_PACE_PLUS_MINUS, p_avg_pace_stats.NBA_FANTASY_PTS AS AVG_PACE_NBA_FANTASY_PTS,
                p_avg_pace_stats.DD2 * 1.0 /p_avg_pace_stats.GP AS AVG_PACE_DD2, p_avg_pace_stats.TD3 * 1.0 /p_avg_pace_stats.GP AS AVG_PACE_TD3,
                
                
                ROUND(team_stats_opponent.OPP_PTS
                + 1.2 * team_stats_opponent.OPP_REB
                + 1.5 * team_stats_opponent.OPP_AST
                + 3 * team_stats_opponent.OPP_BLK
                + 3 * team_stats_opponent.OPP_STL
                + -1 * team_stats_opponent.OPP_TOV, 1) AS OPP_FP,
                
                ROUND(team_stats_opponent.OPP_PTS
                + 1.25 * team_stats_opponent.OPP_REB
                + 1.5 * team_stats_opponent.OPP_AST
                + 2 * team_stats_opponent.OPP_BLK
                + 2 * team_stats_opponent.OPP_STL
                + -0.5 * team_stats_opponent.OPP_TOV
                + 0.5 * team_stats_opponent.OPP_FG3M, 2) AS OPP_DK_FP,
                
                team_stats_opponent.TEAM_NAME AS OPP_TEAM_NAME, team_stats_opponent.W_PCT AS OPP_W_PCT, OPP_FGM, OPP_FGA, OPP_FG_PCT, OPP_FG3M, OPP_FG3A, OPP_FG3_PCT, OPP_FTM, OPP_FTA, OPP_FT_PCT, OPP_OREB, OPP_DREB, OPP_REB, OPP_AST, OPP_TOV, OPP_STL, OPP_BLK, OPP_BLKA, OPP_PF, OPP_PFD, OPP_PTS, team_stats_opponent.PLUS_MINUS AS OPP_PLUS_MINUS, OPP_FGM_RANK, OPP_FGA_RANK, OPP_FG_PCT_RANK, OPP_FG3M_RANK, OPP_FG3A_RANK, OPP_FG3_PCT_RANK, OPP_FTM_RANK, OPP_FTA_RANK, OPP_FT_PCT_RANK, OPP_OREB_RANK, OPP_DREB_RANK, OPP_REB_RANK, OPP_AST_RANK, OPP_TOV_RANK, OPP_STL_RANK, OPP_BLK_RANK, OPP_BLKA_RANK, OPP_PF_RANK, OPP_PFD_RANK, OPP_PTS_RANK, team_stats_opponent.PLUS_MINUS_RANK AS OPP_PLUS_MINUS_RANK,
                
                team_stats.OFF_RATING AS TEAM_OFF_RATING, team_stats.DEF_RATING AS TEAM_DEF_RATING, team_stats.NET_RATING AS TEAM_NET_RATING, team_stats.AST_PCT AS TEAM_AST_PCT, team_stats.AST_TO AS TEAM_AST_TO, team_stats.AST_RATIO AS TEAM_AST_RATIO, team_stats.OREB_PCT AS TEAM_OREB_PCT, team_stats.DREB_PCT AS TEAM_DREB_PCT, team_stats.REB_PCT AS TEAM_REB_PCT, team_stats.TM_TOV_PCT AS TEAM_TM_TOV_PCT, team_stats.EFG_PCT AS TEAM_EFG_PCT, team_stats.TS_PCT AS TEAM_TS_PCT, team_stats.PACE AS TEAM_PACE, team_stats.PIE AS TEAM_PIE,

                opponent_team_stats.OFF_RATING AS OPPONENT_TEAM_OFF_RATING, opponent_team_stats.DEF_RATING AS OPPONENT_TEAM_DEF_RATING, opponent_team_stats.NET_RATING AS OPPONENT_TEAM_NET_RATING, opponent_team_stats.AST_PCT AS OPPONENT_TEAM_AST_PCT, opponent_team_stats.AST_TO AS OPPONENT_TEAM_AST_TO, opponent_team_stats.AST_RATIO AS OPPONENT_TEAM_AST_RATIO, opponent_team_stats.OREB_PCT AS OPPONENT_TEAM_OREB_PCT, opponent_team_stats.DREB_PCT AS OPPONENT_TEAM_DREB_PCT, opponent_team_stats.REB_PCT AS OPPONENT_TEAM_REB_PCT, opponent_team_stats.TM_TOV_PCT AS OPPONENT_TEAM_TM_TOV_PCT, opponent_team_stats.EFG_PCT AS OPPONENT_TEAM_EFG_PCT, opponent_team_stats.TS_PCT AS OPPONENT_TEAM_TS_PCT, opponent_team_stats.PACE AS OPPONENT_TEAM_PACE, opponent_team_stats.PIE AS OPPONENT_TEAM_PIE,
                                
                starters.is_starter AS IS_STARTER,
                
                dnp_stats.DNP_MIN AS TOTAL_DNP_MIN, dnp_stats.DNP_FGM AS TOTAL_DNP_FGM, dnp_stats.DNP_FGA AS TOTAL_DNP_FGA, dnp_stats.DNP_FG3M AS TOTAL_DNP_FG3M, dnp_stats.DNP_FG3A AS TOTAL_DNP_FG3A, dnp_stats.DNP_FTM AS TOTAL_DNP_FTM, dnp_stats.DNP_FTA AS TOTAL_DNP_FTA, dnp_stats.DNP_OREB AS TOTAL_DNP_OREB, dnp_stats.DNP_DREB AS TOTAL_DNP_DREB, dnp_stats.DNP_REB AS TOTAL_DNP_REB, dnp_stats.DNP_AST AS TOTAL_DNP_AST, dnp_stats.DNP_TOV AS TOTAL_DNP_TOV, dnp_stats.DNP_STL AS TOTAL_DNP_STL, dnp_stats.DNP_BLK AS TOTAL_DNP_BLK, dnp_stats.DNP_BLKA AS TOTAL_DNP_BLKA, dnp_stats.DNP_PF AS TOTAL_DNP_PF, dnp_stats.DNP_PFD AS TOTAL_DNP_PFD, dnp_stats.DNP_PTS AS TOTAL_DNP_PTS, dnp_stats.DNP_PLUS_MINUS AS TOTAL_DNP_PLUS_MINUS, dnp_stats.DNP_NBA_FANTASY_PTS AS TOTAL_DNP_NBA_FANTASY_PTS, dnp_stats.DNP_DD2 AS TOTAL_DNP_DD2, dnp_stats.DNP_TD3 AS TOTAL_DNP_TD3,
                
                dnp_stats_by_position.DNP_MIN AS POSITION_DNP_MIN, dnp_stats_by_position.DNP_FGM AS POSITION_DNP_FGM, dnp_stats_by_position.DNP_FGA AS POSITION_DNP_FGA, dnp_stats_by_position.DNP_FG3M AS POSITION_DNP_FG3M, dnp_stats_by_position.DNP_FG3A AS POSITION_DNP_FG3A, dnp_stats_by_position.DNP_FTM AS POSITION_DNP_FTM, dnp_stats_by_position.DNP_FTA AS POSITION_DNP_FTA, dnp_stats_by_position.DNP_OREB AS POSITION_DNP_OREB, dnp_stats_by_position.DNP_DREB AS POSITION_DNP_DREB, dnp_stats_by_position.DNP_REB AS POSITION_DNP_REB, dnp_stats_by_position.DNP_AST AS POSITION_DNP_AST, dnp_stats_by_position.DNP_TOV AS POSITION_DNP_TOV, dnp_stats_by_position.DNP_STL AS POSITION_DNP_STL, dnp_stats_by_position.DNP_BLK AS POSITION_DNP_BLK, dnp_stats_by_position.DNP_BLKA AS POSITION_DNP_BLKA, dnp_stats_by_position.DNP_PF AS POSITION_DNP_PF, dnp_stats_by_position.DNP_PFD AS POSITION_DNP_PFD, dnp_stats_by_position.DNP_PTS AS POSITION_DNP_PTS, dnp_stats_by_position.DNP_PLUS_MINUS AS POSITION_DNP_PLUS_MINUS, dnp_stats_by_position.DNP_NBA_FANTASY_PTS AS POSITION_DNP_NBA_FANTASY_PTS, dnp_stats_by_position.DNP_DD2 AS POSITION_DNP_DD2, dnp_stats_by_position.DNP_TD3 AS POSITION_DNP_TD3,
                
                COALESCE(AVG_INJURED_MIN, p_avg_stats.MIN) AS AVG_INJURED_MIN,
                COALESCE(AVG_INJURED_FGM, p_avg_stats.FGM) AS AVG_INJURED_FGM,
                COALESCE(AVG_INJURED_FGA, p_avg_stats.FGA) AS AVG_INJURED_FGA,
                COALESCE(AVG_INJURED_FG_PCT, p_avg_stats.FG_PCT) AS AVG_INJURED_FG_PCT,
                COALESCE(AVG_INJURED_FG3M, p_avg_stats.FG3M) AS AVG_INJURED_FG3M,
                COALESCE(AVG_INJURED_FG3A, p_avg_stats.FG3A) AS AVG_INJURED_FG3A,
                COALESCE(AVG_INJURED_FG3_PCT, p_avg_stats.FG3_PCT) AS AVG_INJURED_FG3_PCT,
                COALESCE(AVG_INJURED_FTM, p_avg_stats.FTM) AS AVG_INJURED_FTM,
                COALESCE(AVG_INJURED_FTA, p_avg_stats.FTA) AS AVG_INJURED_FTA,
                COALESCE(AVG_INJURED_FT_PCT, p_avg_stats.FT_PCT) AS AVG_INJURED_FT_PCT,
                COALESCE(AVG_INJURED_OREB, p_avg_stats.OREB) AS AVG_INJURED_OREB,
                COALESCE(AVG_INJURED_DREB, p_avg_stats.DREB) AS AVG_INJURED_DREB,
                COALESCE(AVG_INJURED_REB, p_avg_stats.REB) AS AVG_INJURED_REB,
                COALESCE(AVG_INJURED_AST, p_avg_stats.AST) AS AVG_INJURED_AST,
                COALESCE(AVG_INJURED_TOV, p_avg_stats.TOV) AS AVG_INJURED_TOV,
                COALESCE(AVG_INJURED_STL, p_avg_stats.STL) AS AVG_INJURED_STL,
                COALESCE(AVG_INJURED_BLK, p_avg_stats.BLK) AS AVG_INJURED_BLK,
                COALESCE(AVG_INJURED_BLKA, p_avg_stats.BLKA) AS AVG_INJURED_BLKA,
                COALESCE(AVG_INJURED_PF, p_avg_stats.PF) AS AVG_INJURED_PF,
                COALESCE(AVG_INJURED_PFD, p_avg_stats.PFD) AS AVG_INJURED_PFD,
                COALESCE(AVG_INJURED_PTS, p_avg_stats.PTS) AS AVG_INJURED_PTS,
                COALESCE(AVG_INJURED_PLUS_MINUS, p_avg_stats.PLUS_MINUS) AS AVG_INJURED_PLUS_MINUS,
                COALESCE(AVG_INJURED_NBA_FANTASY_PTS, p_avg_stats.NBA_FANTASY_PTS) AS AVG_INJURED_NBA_FANTASY_PTS,
                COALESCE(AVG_INJURED_DD2, p_avg_stats.DD2) AS AVG_INJURED_DD2,
                COALESCE(AVG_INJURED_TD3, p_avg_stats.TD3) AS AVG_INJURED_TD3,

                COALESCE(MAX_INJURED_MIN, p_avg_stats.MIN) AS MAX_INJURED_MIN,
                COALESCE(MAX_INJURED_FGM, p_avg_stats.FGM) AS MAX_INJURED_FGM,
                COALESCE(MAX_INJURED_FGA, p_avg_stats.FGA) AS MAX_INJURED_FGA,
                COALESCE(MAX_INJURED_FG_PCT, p_avg_stats.FG_PCT) AS MAX_INJURED_FG_PCT,
                COALESCE(MAX_INJURED_FG3M, p_avg_stats.FG3M) AS MAX_INJURED_FG3M,
                COALESCE(MAX_INJURED_FG3A, p_avg_stats.FG3A) AS MAX_INJURED_FG3A,
                COALESCE(MAX_INJURED_FG3_PCT, p_avg_stats.FG3_PCT) AS MAX_INJURED_FG3_PCT,
                COALESCE(MAX_INJURED_FTM, p_avg_stats.FTM) AS MAX_INJURED_FTM,
                COALESCE(MAX_INJURED_FTA, p_avg_stats.FTA) AS MAX_INJURED_FTA,
                COALESCE(MAX_INJURED_FT_PCT, p_avg_stats.FT_PCT) AS MAX_INJURED_FT_PCT,
                COALESCE(MAX_INJURED_OREB, p_avg_stats.OREB) AS MAX_INJURED_OREB,
                COALESCE(MAX_INJURED_DREB, p_avg_stats.DREB) AS MAX_INJURED_DREB,
                COALESCE(MAX_INJURED_REB, p_avg_stats.REB) AS MAX_INJURED_REB,
                COALESCE(MAX_INJURED_AST, p_avg_stats.AST) AS MAX_INJURED_AST,
                COALESCE(MAX_INJURED_TOV, p_avg_stats.TOV) AS MAX_INJURED_TOV,
                COALESCE(MAX_INJURED_STL, p_avg_stats.STL) AS MAX_INJURED_STL,
                COALESCE(MAX_INJURED_BLK, p_avg_stats.BLK) AS MAX_INJURED_BLK,
                COALESCE(MAX_INJURED_BLKA, p_avg_stats.BLKA) AS MAX_INJURED_BLKA,
                COALESCE(MAX_INJURED_PF, p_avg_stats.PF) AS MAX_INJURED_PF,
                COALESCE(MAX_INJURED_PFD, p_avg_stats.PFD) AS MAX_INJURED_PFD,
                COALESCE(MAX_INJURED_PTS, p_avg_stats.PTS) AS MAX_INJURED_PTS,
                COALESCE(MAX_INJURED_PLUS_MINUS, p_avg_stats.PLUS_MINUS) AS MAX_INJURED_PLUS_MINUS,
                COALESCE(MAX_INJURED_NBA_FANTASY_PTS, p_avg_stats.NBA_FANTASY_PTS) AS MAX_INJURED_NBA_FANTASY_PTS,
                COALESCE(MAX_INJURED_DD2, p_avg_stats.DD2) AS MAX_INJURED_DD2,
                COALESCE(MAX_INJURED_TD3, p_avg_stats.TD3) AS MAX_INJURED_TD3,
                
                COALESCE(AVG_INJURED_MIN - p_avg_stats.MIN, 0) AS AVG_DIFF_INJURED_MIN,
                COALESCE(AVG_INJURED_FGM - p_avg_stats.FGM, 0) AS AVG_DIFF_INJURED_FGM,
                COALESCE(AVG_INJURED_FGA - p_avg_stats.FGA, 0) AS AVG_DIFF_INJURED_FGA,
                COALESCE(AVG_INJURED_FG_PCT - p_avg_stats.FG_PCT, 0) AS AVG_DIFF_INJURED_FG_PCT,
                COALESCE(AVG_INJURED_FG3M - p_avg_stats.FG3M, 0) AS AVG_DIFF_INJURED_FG3M,
                COALESCE(AVG_INJURED_FG3A - p_avg_stats.FG3A, 0) AS AVG_DIFF_INJURED_FG3A,
                COALESCE(AVG_INJURED_FG3_PCT - p_avg_stats.FG3_PCT, 0) AS AVG_DIFF_INJURED_FG3_PCT,
                COALESCE(AVG_INJURED_FTM - p_avg_stats.FTM, 0) AS AVG_DIFF_INJURED_FTM,
                COALESCE(AVG_INJURED_FTA - p_avg_stats.FTA, 0) AS AVG_DIFF_INJURED_FTA,
                COALESCE(AVG_INJURED_FT_PCT - p_avg_stats.FT_PCT, 0) AS AVG_DIFF_INJURED_FT_PCT,
                COALESCE(AVG_INJURED_OREB - p_avg_stats.OREB, 0) AS AVG_DIFF_INJURED_OREB,
                COALESCE(AVG_INJURED_DREB - p_avg_stats.DREB, 0) AS AVG_DIFF_INJURED_DREB,
                COALESCE(AVG_INJURED_REB - p_avg_stats.REB, 0) AS AVG_DIFF_INJURED_REB,
                COALESCE(AVG_INJURED_AST - p_avg_stats.AST, 0) AS AVG_DIFF_INJURED_AST,
                COALESCE(AVG_INJURED_TOV - p_avg_stats.TOV, 0) AS AVG_DIFF_INJURED_TOV,
                COALESCE(AVG_INJURED_STL - p_avg_stats.STL, 0) AS AVG_DIFF_INJURED_STL,
                COALESCE(AVG_INJURED_BLK - p_avg_stats.BLK, 0) AS AVG_DIFF_INJURED_BLK,
                COALESCE(AVG_INJURED_BLKA - p_avg_stats.BLKA, 0) AS AVG_DIFF_INJURED_BLKA,
                COALESCE(AVG_INJURED_PF - p_avg_stats.PF, 0) AS AVG_DIFF_INJURED_PF,
                COALESCE(AVG_INJURED_PFD - p_avg_stats.PFD, 0) AS AVG_DIFF_INJURED_PFD,
                COALESCE(AVG_INJURED_PTS - p_avg_stats.PTS, 0) AS AVG_DIFF_INJURED_PTS,
                COALESCE(AVG_INJURED_PLUS_MINUS - p_avg_stats.PLUS_MINUS, 0) AS AVG_DIFF_INJURED_PLUS_MINUS,
                COALESCE(AVG_INJURED_NBA_FANTASY_PTS - p_avg_stats.NBA_FANTASY_PTS, 0) AS AVG_DIFF_INJURED_NBA_FANTASY_PTS,
                COALESCE(AVG_INJURED_DD2 - p_avg_stats.DD2, 0) AS AVG_DIFF_INJURED_DD2,
                COALESCE(AVG_INJURED_TD3 - p_avg_stats.TD3, 0) AS AVG_DIFF_INJURED_TD3,
                
                COALESCE(MAX_INJURED_MIN - p_avg_stats.MIN, 0) AS MAX_DIFF_INJURED_MIN,
                COALESCE(MAX_INJURED_FGM - p_avg_stats.FGM, 0) AS MAX_DIFF_INJURED_FGM,
                COALESCE(MAX_INJURED_FGA - p_avg_stats.FGA, 0) AS MAX_DIFF_INJURED_FGA,
                COALESCE(MAX_INJURED_FG_PCT - p_avg_stats.FG_PCT, 0) AS MAX_DIFF_INJURED_FG_PCT,
                COALESCE(MAX_INJURED_FG3M - p_avg_stats.FG3M, 0) AS MAX_DIFF_INJURED_FG3M,
                COALESCE(MAX_INJURED_FG3A - p_avg_stats.FG3A, 0) AS MAX_DIFF_INJURED_FG3A,
                COALESCE(MAX_INJURED_FG3_PCT - p_avg_stats.FG3_PCT, 0) AS MAX_DIFF_INJURED_FG3_PCT,
                COALESCE(MAX_INJURED_FTM - p_avg_stats.FTM, 0) AS MAX_DIFF_INJURED_FTM,
                COALESCE(MAX_INJURED_FTA - p_avg_stats.FTA, 0) AS MAX_DIFF_INJURED_FTA,
                COALESCE(MAX_INJURED_FT_PCT - p_avg_stats.FT_PCT, 0) AS MAX_DIFF_INJURED_FT_PCT,
                COALESCE(MAX_INJURED_OREB - p_avg_stats.OREB, 0) AS MAX_DIFF_INJURED_OREB,
                COALESCE(MAX_INJURED_DREB - p_avg_stats.DREB, 0) AS MAX_DIFF_INJURED_DREB,
                COALESCE(MAX_INJURED_REB - p_avg_stats.REB, 0) AS MAX_DIFF_INJURED_REB,
                COALESCE(MAX_INJURED_AST - p_avg_stats.AST, 0) AS MAX_DIFF_INJURED_AST,
                COALESCE(MAX_INJURED_TOV - p_avg_stats.TOV, 0) AS MAX_DIFF_INJURED_TOV,
                COALESCE(MAX_INJURED_STL - p_avg_stats.STL, 0) AS MAX_DIFF_INJURED_STL,
                COALESCE(MAX_INJURED_BLK - p_avg_stats.BLK, 0) AS MAX_DIFF_INJURED_BLK,
                COALESCE(MAX_INJURED_BLKA - p_avg_stats.BLKA, 0) AS MAX_DIFF_INJURED_BLKA,
                COALESCE(MAX_INJURED_PF - p_avg_stats.PF, 0) AS MAX_DIFF_INJURED_PF,
                COALESCE(MAX_INJURED_PFD - p_avg_stats.PFD, 0) AS MAX_DIFF_INJURED_PFD,
                COALESCE(MAX_INJURED_PTS - p_avg_stats.PTS, 0) AS MAX_DIFF_INJURED_PTS,
                COALESCE(MAX_INJURED_PLUS_MINUS - p_avg_stats.PLUS_MINUS, 0) AS MAX_DIFF_INJURED_PLUS_MINUS,
                COALESCE(MAX_INJURED_NBA_FANTASY_PTS - p_avg_stats.NBA_FANTASY_PTS, 0) AS MAX_DIFF_INJURED_NBA_FANTASY_PTS,
                COALESCE(MAX_INJURED_DD2 - p_avg_stats.DD2, 0) AS MAX_DIFF_INJURED_DD2,
                COALESCE(MAX_INJURED_TD3 - p_avg_stats.TD3, 0) AS MAX_DIFF_INJURED_TD3


                
            FROM PLAYER_LOGS as p_log_today
                INNER JOIN yesterday_date_map
                    ON p_log_today.SEASON = yesterday_date_map.SEASON
                        AND p_log_today.PLAYER_ID = yesterday_date_map.PLAYER_ID
                        AND p_log_today.GAME_DATE = yesterday_date_map.PAST_GAME_DATE
                
                INNER JOIN PLAYER_LOGS AS p_log_future
                    ON p_log_future.SEASON = yesterday_date_map.SEASON
                        AND p_log_future.PLAYER_ID = yesterday_date_map.PLAYER_ID
                        AND p_log_future.GAME_DATE = yesterday_date_map.FUTURE_GAME_DATE
                
                LEFT JOIN AVG_PLAYER_INJURED AS avg_player_injured
                    ON avg_player_injured.SEASON = p_log_today.SEASON
                        AND avg_player_injured.PLAYER_ID = p_log_today.PLAYER_ID
                        AND avg_player_injured.GAME_DATE = p_log_future.GAME_DATE
                
                LEFT JOIN MAX_PLAYER_INJURIES AS max_player_injured
                    ON max_player_injured.SEASON = p_log_today.SEASON
                        AND max_player_injured.PLAYER_ID = p_log_today.PLAYER_ID
                        AND max_player_injured.GAME_DATE = p_log_future.GAME_DATE
                
                INNER JOIN PLAYER_LOGS_ADVANCED AS adv_p_log_today
                    ON adv_p_log_today.SEASON = p_log_today.SEASON
                        AND adv_p_log_today.PLAYER_ID = p_log_today.PLAYER_ID
                        AND adv_p_log_today.GAME_DATE = p_log_today.GAME_DATE
                        
                INNER JOIN GENERAL_TEAM_STATS_OPPONENT_ABBREVS AS team_stats_opponent
                    ON team_stats_opponent.TEAM_ABBREVIATION = SUBSTR(p_log_future.MATCHUP, -3)
                        AND team_stats_opponent.SEASON = p_log_today.SEASON
                        AND team_stats_opponent.DATE_TO = DATE(p_log_future.GAME_DATE, '-1 day')
                        
                INNER JOIN GENERAL_TEAM_STATS_ABBREVS AS opponent_team_stats
                    ON opponent_team_stats.TEAM_ABBREVIATION = SUBSTR(p_log_future.MATCHUP, -3)
                        AND opponent_team_stats.SEASON = p_log_today.SEASON
                        AND opponent_team_stats.DATE_TO = DATE(p_log_future.GAME_DATE, '-1 day')
                
                INNER JOIN GENERAL_TEAM_STATS_ABBREVS AS team_stats
                    ON team_stats.TEAM_ABBREVIATION = SUBSTR(p_log_future.MATCHUP, 1, 3)
                        AND team_stats.SEASON = p_log_today.SEASON
                        AND team_stats.DATE_TO = DATE(p_log_future.GAME_DATE, '-1 day')
                
                INNER JOIN GENERAL_TRADITIONAL_PLAYER_STATS AS p_avg_stats
                    ON p_avg_stats.PLAYER_ID = p_log_today.PLAYER_ID
                        AND p_avg_stats.SEASON = p_log_today.SEASON
                        AND p_avg_stats.DATE_TO = DATE(p_log_future.GAME_DATE, '-1 day')

                INNER JOIN GENERAL_TRADITIONAL_PLAYER_STATS_PACE_ADJUSTED AS p_avg_pace_stats
                    ON p_avg_pace_stats.PLAYER_ID = p_log_today.PLAYER_ID
                        AND p_avg_pace_stats.SEASON = p_log_today.SEASON
                        AND p_avg_pace_stats.DATE_TO = DATE(p_log_future.GAME_DATE, '-1 day')
                
                INNER JOIN STARTERS AS starters
                    ON starters.PLAYER_ID = p_log_today.PLAYER_ID
                        AND starters.SEASON = p_log_today.SEASON
                        AND starters.GAME_ID = p_log_future.GAME_ID
                
                INNER JOIN DNP_STATS AS dnp_stats
                    ON dnp_stats.GAME_ID = p_log_future.GAME_ID
                        AND dnp_stats.SEASON = p_log_today.SEASON
                        AND dnp_stats.TEAM_ID = p_log_today.TEAM_ID
                
                INNER JOIN PLAYER_IDS_TO_MAX_POS AS max_player_pos
                    ON max_player_pos.SEASON = p_log_today.SEASON
                        AND max_player_pos.PLAYER_ID = p_log_today.PLAYER_ID
                
                INNER JOIN DNP_STATS_BY_POSITION AS dnp_stats_by_position
                    ON dnp_stats_by_position.GAME_ID = p_log_future.GAME_ID
                        AND dnp_stats_by_position.SEASON = p_log_today.SEASON
                        AND dnp_stats_by_position.TEAM_ID = p_log_today.TEAM_ID
                        AND dnp_stats_by_position.PLAYER_POSITION = max_player_pos.PLAYER_POSITION
                
                
                
            WHERE 
                p_log_today.SEASON >= ?
            ORDER BY p_log_today.SEASON ASC,
                p_log_today.PLAYER_ID,
                p_log_today.GAME_DATE ASC;
        """, params=(filter_season, ), con=con)


def retrieve_player_logs():
    """
    Utility function to fetch player_logs as a pandas df.
    """
    return db_query("""SELECT * FROM player_logs ORDER BY PLAYER_ID, GAME_DATE, SEASON;""")


def db_query(sql_query: str, params=(), con=None):
    """
    Returns a pandas dataframe corresponding to the result of
    executing the sql_query.

    Optionally pass in a connection to use temporary tables and views.
    """
    if con is not None:
        db_query_result = db.utils.execute_sql_persist(sql_query, con, params=params)
    else:
        db_query_result = db.utils.execute_sql(sql_query, params=params)
    return pd.DataFrame(data=db_query_result.rows, columns=db_query_result.column_names)


def df_to_csv(df: pd.DataFrame, file_name: str):
    """
    Stores a pandas dataframe as a csv file with the given filename.
    Does not store the index of the dataframe and
    sets na (None or null values) as 0.
    """
    OUTPUT_PATH = 'csv_output'
    if not os.path.isdir(OUTPUT_PATH):
        os.makedirs(OUTPUT_PATH)

    df.to_csv('{}/{}.csv'.format(OUTPUT_PATH, file_name), na_rep=0, index=False)


def exists_table(table_name: str):
    """
    Returns True if there already exists a table with this name.
    """
    try:
        # if this errors, then there was not a table with this name
        db.utils.execute_sql("""SELECT * FROM {} LIMIT 1;""".format(table_name))
        return True
    except:
        return False


def get_table_names(only_data=True):
    """
    Returns a list of table names in the database.
    If only_data is False, then all table names are returned
    including tables such as scrape_log and player_ids.
    """
    EXCLUDE_TABLES = {'scrape_log', 'player_ids'}
    table_names = [table_name for table_name in db.utils.get_table_names()
                   if (not only_data or table_name not in EXCLUDE_TABLES)]
    return table_names
