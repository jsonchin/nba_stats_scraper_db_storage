"""
Handles the creation of tables and storage into tables.
"""
from typing import List
from .. import db, CONFIG
from ..scrape.utils import is_proper_date_format, format_date

PROTECTED_COL_NAMES = {'TO'}

DATE_QUERY_PARAMS = {'GAME_DATE', 'DATE_TO'}


def store_nba_response(data_name: str, nba_response, primary_keys=(), ignore_keys=set()):
    """
    Stores a single nba_response, creating a table
    if necessary with the given data_name and primary keys.
    """
    store_nba_responses(data_name, [nba_response], primary_keys, ignore_keys)


def store_nba_responses(data_name: str, l_nba_response: List, primary_keys=(), ignore_keys=set()):
    """
    Stores a given list of nba responses, creating a table
    if necessary with the given data_name and primary keys.
    """
    if len(l_nba_response) == 0:
        raise ValueError('List of nba responses was empty.')

    # process the rows to only contain the desired columns
    combined_ignore_keys = ignore_keys | CONFIG['GLOBAL_IGNORE_KEYS']
    desired_column_headers = [
        header for header in l_nba_response[0].headers if header not in combined_ignore_keys]
    processed_rows = []
    for nba_response in l_nba_response:
        processed_rows.extend(filter_columns(nba_response, desired_column_headers))
    
    # format date
    for header_i, header in enumerate(desired_column_headers):
        if header in DATE_QUERY_PARAMS:
            example_date = processed_rows[0][header_i]
            if not is_proper_date_format(example_date):
                for row_i, row in enumerate(processed_rows):
                    processed_rows[row_i][header_i] = format_date(row[header_i])


    # rename the columns to remove protected names
    renamed_column_headers = rename_protected_col_names(desired_column_headers)

    if db.retrieve.exists_table(data_name):
        add_to_table(data_name, renamed_column_headers, processed_rows)
    else:
        create_table_with_data(data_name, renamed_column_headers, processed_rows, primary_keys)


def filter_columns(nba_response, desired_column_headers):
    """
    Keeps all columns specified in desired_column_headers
    and returns the processed list of rows.
    """
    try:
        desired_column_indicies = [nba_response.headers.index(
            header) for header in desired_column_headers]
    except ValueError:
        raise ValueError('nba response headers are inconsistent: {} \n\n {}'.format(
            nba_response.headers,
            desired_column_headers
        ))

    rows = nba_response.rows
    processed_rows = []
    for row in rows:
        processed_rows.append([row[i] for i in desired_column_indicies])
    return processed_rows


def rename_protected_col_names(column_names: List[str]):
    """
    Returns a new list with renamed columns.
    Renamed columns have a NBA_ prepended.
    """
    return [col_name if col_name not in PROTECTED_COL_NAMES
            else 'NBA_{}'.format(col_name) for col_name in column_names]


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

        unknown_type_indicies = set(range(len(headers)))
        column_types = [None for _ in range(len(headers))]
        r = 0
        while len(unknown_type_indicies) > 0:
            indicies_to_remove = []
            for i in unknown_type_indicies:
                if rows[r][i] is not None:
                    column_types[i] = TYPE_MAPPING[type(rows[r][i])]
                    indicies_to_remove.append(i)
            for i in indicies_to_remove:
                unknown_type_indicies.remove(i)
            r += 1
        return column_types

    def format_column_strs():
        """
        Returns the string representing the declaration of columns
         in a sqlite3 table declaration which includes.
        - column name
        - column type (sqlite3)
        - primary key

        Ex. 'PLAYER_ID INT, PLAYER_NAME TEXT, PRIMARY KEY (PLAYER_ID, PLAYER_NAME)'
        """
        column_types = get_column_types(rows)
        column_strs = []
        for i in range(len(headers)):
            column_str = '{} {}'.format(headers[i], column_types[i])
            column_strs.append(column_str)
        column_def_str = ', '.join(column_strs)
        if len(primary_keys) != 0:
            column_def_str += ', PRIMARY KEY ({})'.format(', '.join(primary_keys))
        return column_def_str

    column_sql_str = format_column_strs()
    db.utils.execute_sql("""CREATE TABLE IF NOT EXISTS {} ({});""".format(table_name, column_sql_str))

    add_to_table(table_name, headers, rows)


def add_to_table(table_name: str, headers: List[str], rows: List[List]):
    """
    Adds the rows to the table.
    """
    insert_values_sql_str = '({})'.format(', '.join(['?'] * len(headers)))
    if CONFIG['IGNORE_DUPLICATES']:
        sql_statement = """INSERT OR IGNORE INTO {} VALUES {};"""
    else:
        sql_statement = """INSERT INTO {} VALUES {};"""

    db.utils.execute_many_sql(sql_statement.format(table_name, insert_values_sql_str), rows)


def is_proper_date_format(date_str):
    """
    Returns True if date_str is in YYYY-MM-DD format.

    >>> is_proper_date_format('2016-10-29')
    True
    >>> is_proper_date_format('11/10/2017')
    False
    >>> is_proper_date_format('OCT 29, 2016')
    False
    >>> is_proper_date_format('2016-10-29T000001')
    False
    """
    try:
        datetime.datetime.strptime(date_str, PROPER_DATE_FORMAT)
        return True
    except ValueError:
        return False


def format_date(date_str):
    """
    Formats the date_str into YYYY-MM-DD format.

    Throws an exception if the date format was unsupported

    Add translations as they show up:
    Currently supported:
    OCT 29, 2016
    YYYY-MM-DD[extra_chars]

    >>> format_date('OCT 29, 2016')
    '2016-10-29'
    >>> format_date('11/10/2017')
    '2017-11-10'
    >>> format_date('2016-10-29T000001')
    '2016-10-29'

    """
    # OCT 29, 2016
    try:
        return datetime.datetime.strftime(
            datetime.datetime.strptime(date_str, '%b %d, %Y'),
            PROPER_DATE_FORMAT
        )
    except ValueError:
        pass

    # MM/DD/YYYY
    try:
        return datetime.datetime.strftime(
            datetime.datetime.strptime(date_str, '%m/%d/%Y'),
            PROPER_DATE_FORMAT
        )
    except ValueError:
        pass

    # YYYY-MM-DD[extra_chars]
    return datetime.datetime.strftime(
        datetime.datetime.strptime(
            date_str[:len(EXAMPLE_PROPER_DATE)], PROPER_DATE_FORMAT),
        PROPER_DATE_FORMAT
    )
