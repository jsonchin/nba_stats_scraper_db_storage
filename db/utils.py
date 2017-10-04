import sqlite3
import db.config as DB_CONFIG


def execute_sql(sql, params=()):
    """
    Executes a sql query and commits the result.
    params is a list of values that will be used
    in place of question marks in the sql statement.
    """
    con = get_db_connection()
    cur = con.execute(sql, params)
    results = cur.fetchall()
    column_names = [description[0] for description in cur.description] if cur.description is not None else None
    close_db_connection(con)
    return column_names, results


def execute_many_sql(sql, seq_of_params):
    """
    Executes a sql statement for a batch of values.
    """
    con = get_db_connection()
    cur = con.executemany(sql, seq_of_params)
    results = cur.fetchall()
    column_names = [description[0] for description in cur.description] if cur.description is not None else None
    close_db_connection(con)
    return column_names, results


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


def get_db_connection():
    con = sqlite3.connect('{}/{}.db'.format(DB_CONFIG.DB_PATH, DB_CONFIG.DB_NAME))
    return con


def close_db_connection(con):
    con.commit()
    con.close()
