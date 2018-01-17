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
from db.config import CSV_OUTPUT_PATH


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
    return db_query_result.to_df()


def df_to_csv(df: pd.DataFrame, file_name: str, csv_output_path=CSV_OUTPUT_PATH):
    """
    Stores a pandas dataframe as a csv file with the given filename.
    Does not store the index of the dataframe and
    sets na (None or null values) as 0.
    """
    if not os.path.isdir(csv_output_path):
        os.makedirs(csv_output_path)

    df.to_csv('{}/{}.csv'.format(csv_output_path, file_name), na_rep=0, index=False)


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
