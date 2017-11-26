import db.initialize
import scrape.scraper

# initialize the database and run the initial scrapes that
# are necessary for this tool (player_ids and game_dates)
db.initialize.init_db()
scrape.scraper.run_scrape_jobs('scrape/api_requests_init.yaml')