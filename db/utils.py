import sqlite3
import db.config as DB_CONFIG

con = sqlite3.connect('{}/{}.db'.format(DB_CONFIG.DB_PATH, DB_CONFIG.DB_NAME))

def execute_sql(sql, params=()):
    """
    Executes a sql query and commits the result.
    params is a list of values that will be used
    in place of question marks in the sql statement.
    """
    cur = con.execute(sql, params)
    con.commit()
    return cur.fetchall()

def execute_many_sql(sql, seq_of_params):
    """
    Executes a sql statement for a batch of values.
    """
    cur = con.executemany(sql, seq_of_params)
    con.commit()
    return cur.fetchall()

def close_con():
    con.close()