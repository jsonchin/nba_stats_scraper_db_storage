"""
Initializes the database and adds a table called "scrape_log"
detailing when requests were made for logging purposes and
to prevent double scraping.
"""

from db_storage import db_utils

def init_db():
    """
    Creates a database with the given name and creates
    the "scrape_log" table.
    """
    db_utils.execute_sql("""CREATE TABLE IF NOT EXISTS scrape_log (date text, api_request text);""")
