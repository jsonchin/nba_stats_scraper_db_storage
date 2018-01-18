from config import CONFIG
CONFIG['DB_NAME'] = 'test_db'
CONFIG['DB_PATH'] = 'tests/test_db/databases'
from db.initialize import init_db
import db.utils

def init_test_db():
    db.utils.drop_tables()
    init_db()
    db.utils.execute_sql("""CREATE TABLE IF NOT EXISTS
                                player_ids (PLAYER_ID text, PLAYER_NAME text, SEASON text);""")
