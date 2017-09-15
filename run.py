from db_storage import db_init
from scraper import scraper_init

if __name__ == '__main__':
    db_init.init_db()
    scraper_init.scrape_player_ids()