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
    
    response_columns = l_nba_response[0].headers
    desired_column_headers = filter_column_headers(response_columns, ignore_keys)
    extracted_rows = extract_used_columns(l_nba_response, desired_column_headers)
    format_date_columns(desired_column_headers, extracted_rows)
    renamed_column_headers = map_protected_col_names(desired_column_headers)

    if db.retrieve.exists_table(data_name):
        add_rows_to_table(data_name, renamed_column_headers, extracted_rows)
    else:
        create_table_with_data(data_name, renamed_column_headers, extracted_rows, primary_keys)


def filter_column_headers(column_names, ignore_keys):
    combined_ignore_keys = ignore_keys | set(CONFIG['GLOBAL_IGNORE_KEYS'])
    desired_column_headers = [
        header for header in column_names
        if header not in combined_ignore_keys
    ]
    return desired_column_headers


def extract_used_columns(nba_responses, desired_column_headers):
    filtered_rows = []
    for nba_response in nba_responses:
        filtered_rows.extend(extract_used_columns_from_nba_response(
            nba_response, desired_column_headers))
    return filtered_rows


def extract_used_columns_from_nba_response(nba_response, desired_column_headers):
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
    filtered_rows = []
    for row in rows:
        filtered_rows.append([row[i] for i in desired_column_indicies])
    return filtered_rows


def format_date_columns(desired_column_headers, rows):
    for header_i, header in enumerate(desired_column_headers):
        if header in DATE_QUERY_PARAMS:
            example_date = rows[0][header_i]
            if not is_proper_date_format(example_date):
                format_date_column(rows, header_i)


def format_date_column(rows, header_i):
    for row_i, row in enumerate(rows):
        rows[row_i][header_i] = format_date(row[header_i])


def map_protected_col_names(column_names: List[str]):
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

    initialize_table_if_not_exists(table_name, headers, rows, primary_keys)
    add_rows_to_table(table_name, headers, rows)


def initialize_table_if_not_exists(table_name, headers, rows, primary_keys):
    column_sql_str = format_sql_table_column_declaration_str(headers, primary_keys, rows)
    db.utils.execute_sql("""CREATE TABLE IF NOT EXISTS {} ({});""".format(
        table_name, column_sql_str))


def format_sql_table_column_declaration_str(headers, primary_keys, rows):
    """
    Returns the string representing the declaration of columns
        in a sqlite3 table declaration which includes.
    - column name
    - column type (sqlite3)
    - primary key

    Ex. 'PLAYER_ID INT, PLAYER_NAME TEXT, PRIMARY KEY (PLAYER_ID, PLAYER_NAME)'
    """
    column_types = get_column_types(headers, rows)
    column_name_type_pairs = ['{} {}'.format(headers[i], column_types[i]) for i in range(len(headers))]
    column_def = ', '.join(column_name_type_pairs)
    if len(primary_keys) != 0:
        column_def += ', PRIMARY KEY ({})'.format(', '.join(primary_keys))
    return column_def


def get_column_types(headers, rows: List[List]):
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
        discovered_type_indicies = []
        for i in unknown_type_indicies:
            if rows[r][i] is not None:
                column_types[i] = TYPE_MAPPING[type(rows[r][i])]
                discovered_type_indicies.append(i)
        for i in discovered_type_indicies:
            unknown_type_indicies.remove(i)
        r += 1
    return column_types


def add_rows_to_table(table_name: str, headers: List[str], rows: List[List]):
    """
    Adds the rows to the table.
    """
    insert_values_sql_str = '({})'.format(', '.join(['?'] * len(headers)))
    if CONFIG['IGNORE_DUPLICATES']:
        sql_statement = """INSERT OR IGNORE INTO {} VALUES {};"""
    else:
        sql_statement = """INSERT OR REPLACE INTO {} VALUES {};"""

    db.utils.execute_many_sql(sql_statement.format(table_name, insert_values_sql_str), rows)
