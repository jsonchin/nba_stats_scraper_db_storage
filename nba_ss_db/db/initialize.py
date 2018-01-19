"""
Initializes the database and adds a table called "scrape_log"
detailing when requests were made for logging purposes and
to prevent double scraping.
"""

from .. import db

def init_db():
    """
    Creates a database with the given name and creates
    the "scrape_log" table.
    """
    db.utils.execute_sql("""CREATE TABLE IF NOT EXISTS scrape_log (date text, api_request text, table_name text);""")
