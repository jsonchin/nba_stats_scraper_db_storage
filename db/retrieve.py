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
    game_ids_by_season = defaultdict(list)
    season_game_date_tuples = db.utils.execute_sql("""SELECT SEASON, GAME_DATE FROM game_dates;""").rows

    for season, game_date in season_game_date_tuples:
        game_ids_by_season[season].append(game_date)

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
                 ROUND(p_log_today.PTS
                + 1.2 * p_log_today.REB
                + 1.5 * p_log_today.AST
                + 3 * p_log_today.BLK
                + 3 * p_log_today.STL
                + -1 * p_log_today.TOV, 1) AS FP,
                p_log_today.*,
                
                adv_p_log_today.OFF_RATING, adv_p_log_today.DEF_RATING, adv_p_log_today.NET_RATING,
                adv_p_log_today.AST_PCT, adv_p_log_today.AST_TO, adv_p_log_today.AST_RATIO,
                adv_p_log_today.OREB_PCT, adv_p_log_today.DREB_PCT, adv_p_log_today.REB_PCT,
                adv_p_log_today.TM_TOV_PCT, adv_p_log_today.EFG_PCT, adv_p_log_today.TS_PCT,
                adv_p_log_today.USG_PCT, adv_p_log_today.PACE, adv_p_log_today.PIE,
                adv_p_log_today.FGM_PG, adv_p_log_today.FGA_PG
                
            FROM PLAYER_LOGS as p_log_today
                INNER JOIN (SELECT p_log1.SEASON AS SEASON,
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
                                                GROUP BY inner_player_logs.PLAYER_ID))
                        AS yesterday_date_map
                    ON p_log_today.SEASON = yesterday_date_map.SEASON
                        AND p_log_today.PLAYER_ID = yesterday_date_map.PLAYER_ID
                        AND p_log_today.GAME_DATE = yesterday_date_map.PAST_GAME_DATE
                
                INNER JOIN PLAYER_LOGS AS p_log_future
                    ON p_log_future.SEASON = yesterday_date_map.SEASON
                        AND p_log_future.PLAYER_ID = yesterday_date_map.PLAYER_ID
                        AND p_log_future.GAME_DATE = yesterday_date_map.FUTURE_GAME_DATE
                
                INNER JOIN ADVANCED_PLAYER_LOGS AS adv_p_log_today
                    ON adv_p_log_today.SEASON = p_log_today.SEASON
                        AND adv_p_log_today.PLAYER_ID = p_log_today.PLAYER_ID
                        AND adv_p_log_today.GAME_DATE = p_log_today.GAME_DATE
                    
                
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
