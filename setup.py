from setuptools import setup

with open('requirements.txt') as f:
    required = f.read().splitlines()

setup(
    name='NBA Stats Scraper + DB Storage',
    version='0.0',
    packages=['db', 'scrape'],
    description='Demo Modules',
    url='https://github.com/jsonchin/nba_stats_scraper_db_storage',
    license='MIT',
    long_description=open('README.md').read(),
    install_requires=required
)