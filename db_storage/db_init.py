"""
Initializes the database and adds a table called "scrape_log"
detailing when requests were made for logging purposes and
to prevent double scraping.
"""

import sqlite3
import db_storage.db_config as DB_CONFIG

def init_db(db_name: str):
    """
    Creates a database with the given name and creates
    the "scrape_log" table.
    """
    con = sqlite3.connect('{}/{}.db'.format(DB_CONFIG.DB_PATH, db_name))
    con.execute("""CREATE TABLE IF NOT EXISTS scrape_log (date text, api_request text);""")
    con.close()
