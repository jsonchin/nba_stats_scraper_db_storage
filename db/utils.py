import sqlite3
import db.config as DB_CONFIG
import datetime
from scrape.utils import PROPER_DATE_FORMAT


class DB_Query():

    def __init__(self, column_names, rows):
        self.column_names = column_names
        self.rows = rows


def execute_sql(sql, params=()):
    """
    Executes a sql query and commits the result.
    params is a list of values that will be used
    in place of question marks in the sql statement.

    Returns a DB_Query.
    """
    if type(params) != tuple and type(params) != list:
        params = (params, )
    con = get_db_connection()
    cur = con.execute(sql, params)
    results = cur.fetchall()
    column_names = [description[0] for description in cur.description] if cur.description is not None else None
    close_db_connection(con)
    return DB_Query(column_names, results)


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


def execute_sql_file(file_name):
    """
    Executes sql in the given file.
    """
    with open(file_name, 'r') as f:
        for cmd in f.read().split(';'):
            execute_sql(cmd)


def get_db_connection():
    con = sqlite3.connect('{}/{}.db'.format(DB_CONFIG.DB_PATH, DB_CONFIG.DB_NAME))
    return con


def close_db_connection(con):
    con.commit()
    con.close()
