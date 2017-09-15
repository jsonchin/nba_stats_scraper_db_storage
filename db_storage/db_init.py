"""
Initializes the database and adds a table called "scrape_log"
detailing when requests were made for logging purposes and
to prevent double scraping.
"""

def init_db(db_name: str):
    """
    Creates a database with the given name and creates
    the "scrape_log" table.
    """