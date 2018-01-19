from setuptools import setup

with open('requirements.txt') as f:
    required = f.read().splitlines()

setup(
    name='NBA Stats Scraper + DB Storage',
    version='0.0',
    packages=['nba_ss_db'],
    description='A tool to automatically scrape and store stats.nba.com data into a database.',
    url='https://github.com/jsonchin/nba_stats_scraper_db_storage',
    license='MIT',
    long_description=open('README.md').read(),
    install_requires=required
)
