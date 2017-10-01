from setuptools import setup
import db.initialize
import scrape.scraper

with open('requirements.txt') as f:
    required = f.read().splitlines()

setup(
    name='NBA Stats Scraper + DB Storage',
    version='0.0',
    packages=['db', 'scrape'],
    description='A tool to automatically scrape and store stats.nba.com data into a database.',
    url='https://github.com/jsonchin/nba_stats_scraper_db_storage',
    license='MIT',
    long_description=open('README.md').read(),
    install_requires=required
)

# initialize the database and run the initial scrapes that
# are necessary for this tool (player_ids and game_dates)
db.initialize.init_db()
scrape.scraper.run_scrape_jobs('scrape/api_requests_init.yaml')