"""
Defines ease of use retrieval functions such as for fetching
all player_ids which occurs often, as well as a general function
for querying with standard SQL.

The general query function will return a pandas dataframe.
"""
import db.utils
from collections import defaultdict
import pandas as pd


def fetch_player_ids():
    """
    Returns a mapping of season to a list of player_ids.
    """
    player_ids_by_season = defaultdict(list)
    season_player_id_tuples = db.utils.execute_sql("""SELECT SEASON, PLAYER_ID FROM player_ids;""")[1]

    for season, player_id in season_player_id_tuples:
        player_ids_by_season[season].append(player_id)

    return player_ids_by_season


def fetch_game_dates():
    """
    Returns a mapping of season to a list of game dates.
    """
    game_ids_by_season = defaultdict(list)
    season_game_date_tuples = db.utils.execute_sql("""SELECT SEASON, GAME_DATE FROM game_dates;""")[1]

    for season, game_date in season_game_date_tuples:
        game_ids_by_season[season].append(game_date)

    return game_ids_by_season


def db_query(sql_query: str):
    """
    Returns a pandas dataframe corresponding to the result of
    executing the sql_query.
    """
    column_names, rows = db.utils.execute_sql(sql_query)
    return pd.DataFrame(data=rows, columns=column_names)


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
    table_names = [l[0] for l in db.utils.execute_sql("""SELECT name FROM sqlite_master WHERE type='table';""")[1]
                   if (not only_data or l[0] not in EXCLUDE_TABLES)]
    return table_names
