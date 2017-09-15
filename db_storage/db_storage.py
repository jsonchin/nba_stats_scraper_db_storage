"""
Handles the creation of tables and storage into tables.
"""

from typing import List
from scraper.nba_response import NBA_response
from db_storage import db_utils

def store_nba_responses(data_name, l_nba_response: List[NBA_response], primary_keys=(), ignore_keys=set()):
    """
    Stores a given list of nba responses, creating a table
    if necessary with the given data_name.
    """
    if len(l_nba_response) == 0:
        raise ValueError('List of nba responses was empty.')

    def filter_columns(nba_response, desired_column_headers):
        """
        Removes all columns specified in ignore_keys
        and returns the processed list of rows.
        """
        try:
            desired_column_indicies = [headers.index(header) for header in desired_column_headers]
        except:
            raise ValueError('nba response headers are inconsistent: {} \n\n {}'.format(
                nba_response.headers,
                headers
            ))

        rows = nba_response.rows
        processed_rows = []
        for row in rows:
            processed_rows.append([row[i] for i in desired_column_indicies])
        return processed_rows

    # process the rows to only contain the desired columns
    headers = l_nba_response[0].headers
    desired_column_headers = [header for header in headers if header not in ignore_keys]

    processed_rows = []
    for nba_response in l_nba_response:
        processed_rows.extend(filter_columns(nba_response, desired_column_headers))


    if exists_table(data_name):
        add_to_table(data_name, processed_rows)
    else:
        create_table_with_data(data_name, desired_column_headers, processed_rows, primary_keys)

def create_table_with_data(table_name: str, headers: List[str], rows: List[List], primary_keys=()):
    """
    Creates a table with column names and rows corresponding
    to the provided json responses.
    """
    if len(headers) != len(rows[0]):
        raise ValueError('Length of the headers and rows are not the same.')

    def get_column_types(rows: List[List]):
        """
        Returns a list of sqlite3 types defined by the
        data in the json response rows.
        """
        TYPE_MAPPING = {
            str: 'TEXT',
            int: 'INT',
            float: 'FLOAT',
            bool: 'INT'
        }
        return [TYPE_MAPPING[type(ele)] for ele in rows[0]]

    def format_column_strs():
        """
        Returns a list of formatted column strings that include:
        - column name
        - column type (sqlite3)
        - primary key (if it is one)

        Ex. ['PLAYER_ID INT PRIMARY KEY', 'PLAYER_NAME TEXT PRIMARY KEY']
        """
        column_types = get_column_types(rows)
        column_strs = []
        for i in range(len(headers)):
            column_str = '{} {}'.format(headers[i], column_types[i])
            if headers[i] in primary_keys:
                column_str += ' PRIMARY KEY'
            column_strs.append(column_str)
        return column_strs

    column_strs = format_column_strs()
    column_sql_str = ', '.join(column_strs)

    primary_key_sql_str = ', '.join(primary_keys)

    db_utils.execute_sql("""CREATE TABLE IF NOT EXISTS {} ({});""".format(
        table_name, column_sql_str, primary_key_sql_str))

    add_to_table(table_name, headers, rows)



def exists_table(table_name: str):
    """
    Returns True if there already exists a table with this name.
    """
    try:
        # if this errors, then there was not a table with this name
        db_utils.execute_sql("""SELECT * FROM {} LIMIT 1;""".format(table_name))
        return False
    except:
        return True

def add_to_table(table_name: str, headers: List[str], rows: List[List]):
    """
    Adds the rows to the table.
    """
    insert_values_sql_str = '({})'.format(', '.join(['?'] * len(headers)))
    db_utils.execute_many_sql("""INSERT INTO {} VALUES {};""".format(table_name, insert_values_sql_str), rows)