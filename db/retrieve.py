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
from scrape.utils import get_date_before, format_date_for_api_request


def fetch_player_ids():
    """
    Returns a mapping of season to a list of player_ids.
    """
    player_ids_by_season = defaultdict(list)
    season_player_id_tuples = db.utils.execute_sql("""SELECT SEASON, PLAYER_ID FROM player_ids;""").rows

    for season, player_id in season_player_id_tuples:
        player_ids_by_season[season].append(player_id)

    return player_ids_by_season


def fetch_game_dates(day_before=False, format_api_request=False):
    """
    Returns a mapping of season to a list of game dates.
    """
    game_ids_by_season = defaultdict(list)
    season_game_date_tuples = db.utils.execute_sql("""SELECT SEASON, GAME_DATE FROM game_dates;""").rows

    for season, game_date in season_game_date_tuples:
        if day_before:
            date = get_date_before(game_date)
        else:
            date = game_date

        if format_api_request:
            date = format_date_for_api_request(date)

        game_ids_by_season[season].append(date)


    return game_ids_by_season


def fetch_game_ids():
    """
    Returns a mapping of season to a list of game ids.
    """
    game_ids_by_season = defaultdict(list)
    season_game_id_tuples = db.utils.execute_sql("""SELECT SEASON, GAME_ID FROM game_dates;""").rows

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


def aggregate_training_data(filter_fp=-10):
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
                
                p_log_future.PTS AS PTS_TO_PREDICT,
                p_log_future.REB AS REB_TO_PREDICT,
                p_log_future.AST AS AST_TO_PREDICT,
                p_log_future.BLK AS BLK_TO_PREDICT,
                p_log_future.STL AS STL_TO_PREDICT,
                p_log_future.TOV AS TOV_TO_PREDICT,
                
                 ROUND(p_log_today.PTS
                + 1.2 * p_log_today.REB
                + 1.5 * p_log_today.AST
                + 3 * p_log_today.BLK
                + 3 * p_log_today.STL
                + -1 * p_log_today.TOV, 1) AS FP,
                
                p_log_today.*,
                
                p_avg_stats.W_PCT AS AVG_W_PCT, p_avg_stats.MIN AS AVG_MIN, p_avg_stats.FGM AS AVG_FGM, p_avg_stats.FGA AS AVG_FGA, p_avg_stats.FG_PCT AS AVG_FG_PCT, p_avg_stats.FG3M AS AVG_FG3M, p_avg_stats.FG3A AS AVG_FG3A, p_avg_stats.FG3_PCT AS AVG_FG3_PCT, p_avg_stats.FTM AS AVG_FTM, p_avg_stats.FTA AS AVG_FTA, p_avg_stats.FT_PCT AS AVG_FT_PCT, p_avg_stats.OREB AS AVG_OREB, p_avg_stats.DREB AS AVG_DREB, p_avg_stats.REB AS AVG_REB, p_avg_stats.AST AS AVG_AST, p_avg_stats.TOV AS AVG_TOV, p_avg_stats.STL AS AVG_STL, p_avg_stats.BLK AS AVG_BLK, p_avg_stats.BLKA AS AVG_BLKA, p_avg_stats.PF AS AVG_PF, p_avg_stats.PFD AS AVG_PFD, p_avg_stats.PTS AS AVG_PTS, p_avg_stats.PLUS_MINUS AS AVG_PLUS_MINUS, p_avg_stats.NBA_FANTASY_PTS AS AVG_NBA_FANTASY_PTS, p_avg_stats.DD2 AS AVG_DD2, p_avg_stats.TD3 AS AVG_TD3,
                
                
                OFF_RATING, DEF_RATING, NET_RATING,
                AST_PCT, AST_TO, AST_RATIO,
                OREB_PCT, DREB_PCT, REB_PCT,
                TM_TOV_PCT, EFG_PCT, TS_PCT,
                USG_PCT, PACE, PIE,
                FGM_PG, FGA_PG,
                
                ROUND(team_stats_opponent.OPP_PTS
                + 1.2 * team_stats_opponent.OPP_REB
                + 1.5 * team_stats_opponent.OPP_AST
                + 3 * team_stats_opponent.OPP_BLK
                + 3 * team_stats_opponent.OPP_STL
                + -1 * team_stats_opponent.OPP_TOV, 1) AS OPP_FP,
                
                team_stats_opponent.TEAM_NAME AS OPP_TEAM_NAME, team_stats_opponent.W_PCT AS OPP_W_PCT, OPP_FGM, OPP_FGA, OPP_FG_PCT, OPP_FG3M, OPP_FG3A, OPP_FG3_PCT, OPP_FTM, OPP_FTA, OPP_FT_PCT, OPP_OREB, OPP_DREB, OPP_REB, OPP_AST, OPP_TOV, OPP_STL, OPP_BLK, OPP_BLKA, OPP_PF, OPP_PFD, OPP_PTS, team_stats_opponent.PLUS_MINUS AS OPP_PLUS_MINUS, OPP_FGM_RANK, OPP_FGA_RANK, OPP_FG_PCT_RANK, OPP_FG3M_RANK, OPP_FG3A_RANK, OPP_FG3_PCT_RANK, OPP_FTM_RANK, OPP_FTA_RANK, OPP_FT_PCT_RANK, OPP_OREB_RANK, OPP_DREB_RANK, OPP_REB_RANK, OPP_AST_RANK, OPP_TOV_RANK, OPP_STL_RANK, OPP_BLK_RANK, OPP_BLKA_RANK, OPP_PF_RANK, OPP_PFD_RANK, OPP_PTS_RANK, team_stats_opponent.PLUS_MINUS_RANK AS OPP_PLUS_MINUS_RANK
                
            FROM PLAYER_LOGS as p_log_today
                INNER JOIN yesterday_date_map
                    ON p_log_today.SEASON = yesterday_date_map.SEASON
                        AND p_log_today.PLAYER_ID = yesterday_date_map.PLAYER_ID
                        AND p_log_today.GAME_DATE = yesterday_date_map.PAST_GAME_DATE
                
                INNER JOIN PLAYER_LOGS AS p_log_future
                    ON p_log_future.SEASON = yesterday_date_map.SEASON
                        AND p_log_future.PLAYER_ID = yesterday_date_map.PLAYER_ID
                        AND p_log_future.GAME_DATE = yesterday_date_map.FUTURE_GAME_DATE
                
                INNER JOIN PLAYER_LOGS_ADVANCED AS adv_p_log_today
                    ON adv_p_log_today.SEASON = p_log_today.SEASON
                        AND adv_p_log_today.PLAYER_ID = p_log_today.PLAYER_ID
                        AND adv_p_log_today.GAME_DATE = p_log_today.GAME_DATE
                        
                INNER JOIN GENERAL_TEAM_STATS_OPPONENT_ABBREVS AS team_stats_opponent
                    ON team_stats_opponent.TEAM_ABBREVIATION = SUBSTR(p_log_future.MATCHUP, -3)
                        AND team_stats_opponent.SEASON = p_log_today.SEASON
                        AND team_stats_opponent.DATE_TO = DATE(p_log_future.GAME_DATE, '-1 day')
                
                INNER JOIN GENERAL_TRADITIONAL_PLAYER_STATS AS p_avg_stats
                    ON p_avg_stats.PLAYER_ID = p_log_today.PLAYER_ID
                        AND p_avg_stats.SEASON = p_log_today.SEASON
                        AND p_avg_stats.DATE_TO = DATE(p_log_future.GAME_DATE, '-1 day')
                    
                
            WHERE (SELECT AVG(
                        p_log.PTS
                        + 1.2 * p_log.REB
                        + 1.5 * p_log.AST
                        + 3 * p_log.BLK
                        + 3 * p_log.STL
                        + -1 * p_log.TOV) FROM player_logs AS p_log
                    WHERE p_log.SEASON = p_log_today.SEASON
                        AND p_log.PLAYER_ID = p_log_today.PLAYER_ID) >= ? 
            ORDER BY p_log_today.SEASON ASC,
                p_log_today.PLAYER_ID,
                p_log_today.GAME_DATE ASC;
        """, params=(filter_fp, ))


def retrieve_player_logs():
    """
    Utility function to fetch player_logs as a pandas df.
    """
    return db_query("""SELECT * FROM player_logs ORDER BY PLAYER_ID, GAME_DATE, SEASON;""")


def db_query(sql_query: str, params=()):
    """
    Returns a pandas dataframe corresponding to the result of
    executing the sql_query.
    """
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
