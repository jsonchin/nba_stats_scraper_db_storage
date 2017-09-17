import sqlite3
import db.db_config as DB_CONFIG

con = sqlite3.connect('{}/{}.db'.format(DB_CONFIG.DB_PATH, DB_CONFIG.DB_NAME))

def execute_sql(sql, params=[]):
    cur = con.execute(sql, params)
    con.commit()
    return cur.fetchall()

def execute_many_sql(sql, seq_of_params):
    cur = con.executemany(sql, seq_of_params)
    con.commit()
    return cur.fetchall()

def close_con():
    con.close()