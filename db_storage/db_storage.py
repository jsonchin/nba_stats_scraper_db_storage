"""
Handles the creation of tables and storage into tables.
"""

from typing import List
from scraper.nba_response import NBA_response

def create_table(l_nba_response: List[NBA_response], primary_keys=()):
    """
    Creates a table with column names and rows corresponding
    to the provided json responses.
    """
    def get_column_types(l_nba_response: List[NBA_response]):
        """
        Returns a list of types defined by the data in the json response
        rows.
        """

def exists_table(table_name: str):
    """
    Returns True if there already exists a table with this name.
    """

def add_to_table(table_name: str, l_nba_response: List[NBA_response]):
    """
    Adds the json responses' rowSets to the table.
    """