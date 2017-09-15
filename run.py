from db_storage import db_init
from scraper import scraper_init
from db_storage import db_utils

if __name__ == '__main__':
    db_init.init_db()
    scraper_init.scrape_player_ids()

    db_utils.close_con()
