import db_storage.db_config as DB_CONFIG
from db_storage import db_init

if __name__ == '__main__':
    db_init.init_db(DB_CONFIG.DB_NAME)