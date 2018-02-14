import nba_ss_db.db.initialize
import nba_ss_db.scrape.scraper

# initialize the database and run the initial scrapes that
# are necessary for this tool (player_ids and game_dates)
nba_ss_db.db.initialize.init_db()
nba_ss_db.scrape.scraper.run_scrape_jobs(
    'nba_ss_db/scrape/api_requests_init.yaml')
