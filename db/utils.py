import sqlite3
import pandas as pd
import db.config as DB_CONFIG
import datetime
from scrape.utils import PROPER_DATE_FORMAT


class DB_Query():

    def __init__(self, column_names, rows):
        self.column_names = column_names
        self.rows = rows

    def to_df(self):
        return pd.DataFrame(self.rows, columns=self.column_names)

def execute_sql(sql, input_con=None, params=()):
    """
    Executes a sql query and commits the result.
    params is a list of values that will be used
    in place of question marks in the sql statement.

    The input_con parameter is useful for creating
    temporary tables that will be persisted across a connection.

    Returns a DB_Query.
    """
    if input_con is None:
        con = get_db_connection()
    else:
        con = input_con

    if type(params) != tuple and type(params) != list:
        params = (params, )
    cur = con.execute(sql, params)
    results = cur.fetchall()
    column_names = [description[0] for description in cur.description] if cur.description is not None else None

    if input_con is None:
        close_db_connection(con)

    return DB_Query(column_names, results)

execute_sql_persist = execute_sql

def execute_many_sql(sql, seq_of_params):
    """
    Executes a sql statement for a batch of values.

    Returns a DB_Query.
    """
    con = get_db_connection()
    cur = con.executemany(sql, seq_of_params)
    results = cur.fetchall()
    column_names = [description[0] for description in cur.description] if cur.description is not None else None
    close_db_connection(con)
    return DB_Query(column_names, results)


def get_table_column_names(table_name: str):
    """
    Returns a list of column names of the specified table.

    This function is in this module because it deals with
    the cursor and connection abstractipn layer.
    """
    con = get_db_connection()
    cur = con.execute("""SELECT * FROM {} LIMIT 1;""".format(table_name))
    column_names = [description[0] for description in cur.description]
    return column_names


def clear_scrape_logs(date=None):
    """
    Removes all entries from the table scrape_log
    that come before the given date.

    date must be in YYYY-MM-DD format, otherwise a ValueError
    will be thrown.
    """
    if date is None:
        execute_sql("""DELETE FROM scrape_log WHERE TRUE;""")
    else:
        try:
            datetime.datetime.strptime(date, PROPER_DATE_FORMAT)
        except ValueError:
            raise ValueError('date was not in the correct format: YYYY-MM-DD')
        execute_sql("""DELETE FROM scrape_log WHERE date < ?;""", date)


def drop_tables():
    """
    Drops all tables in the database.
    """
    table_names = [l[0] for l in execute_sql("""SELECT name FROM sqlite_master WHERE type='table';""").rows]
    for table_name in table_names:
        execute_sql("""DROP TABLE {};""".format(table_name))


def get_table_names():
    """
    Returns a list of table names (list of strings).
    """
    return [l[0] for l in execute_sql("""SELECT name FROM sqlite_master WHERE type='table';""").rows]


def execute_sql_file(file_name, input_con=None):
    """
    Executes sql in the given file.
    Returns the output of the last sql statement in the file.
    Statements are separated by the semicolon (;) character.
    """
    if input_con is None:
        con = get_db_connection()
    else:
        con = input_con
    output = None
    with open(file_name, 'r') as f:
        for cmd in f.read().split(';')[:-1]:
            try:
                output = execute_sql(cmd, con)
            except Exception as e:
                print('Tried to execute this command but failed: {}'.format(cmd))
                raise e
        # close the con if it was created by this function
        if input_con is None:
            close_db_connection(con)
    return output

execute_sql_file_persist = execute_sql_file


def get_db_connection():
    con = sqlite3.connect('{}/{}.db'.format(DB_CONFIG.DB_PATH, DB_CONFIG.DB_NAME))
    return con


def close_db_connection(con):
    con.commit()
    con.close()
