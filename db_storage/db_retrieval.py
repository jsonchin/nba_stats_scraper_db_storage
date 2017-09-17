"""
Defines ease of use retrieval functions such as for fetching
all player_ids which occurs often, as well as a general function
for querying with standard SQL.

The general query function will return a pandas dataframe.
"""

from db_storage import db_utils
import pandas as pd

def fetch_player_ids():
    """
    Returns a list of player_ids.
    """
    return [row[0] for row in db_utils.execute_sql("""SELECT PLAYER_ID FROM player_ids;""")]

def db_query(sql_query: str):
    """
    Returns a pandas dataframe corresponding to the result of
    executing the sql_query.
    """