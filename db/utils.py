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
    close_db_connection(con)
    return results


def execute_many_sql(sql, seq_of_params):
    """
    Executes a sql statement for a batch of values.
    """
    con = get_db_connection()
    cur = con.executemany(sql, seq_of_params)
    results = cur.fetchall()
    close_db_connection(con)
    return results


def get_db_connection():
    con = sqlite3.connect('{}/{}.db'.format(DB_CONFIG.DB_PATH, DB_CONFIG.DB_NAME))
    return con


def close_db_connection(con):
    con.commit()
    con.close()
