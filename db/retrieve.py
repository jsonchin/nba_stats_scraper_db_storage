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
    Returns a list of player_ids.
    """
    player_ids_by_season = defaultdict(list)
    season_player_id_tuples = db.utils.execute_sql("""SELECT SEASON, PLAYER_ID FROM player_ids;""")

    for season, player_id in season_player_id_tuples:
        player_ids_by_season[season].append(player_id)

    return player_ids_by_season

def db_query(sql_query: str):
    """
    Returns a pandas dataframe corresponding to the result of
    executing the sql_query.
    """

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