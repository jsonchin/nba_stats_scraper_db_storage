"""
Defines common scraper patterns to be reused.

Ex. Scrape all ___ for each season
Ex. Scrape all ___ for each season for each position
Ex. Scrape all ___ for each season for each player_id
"""
import scraper.scraper_config as SCRAPER_CONFIG
from scraper import scraper_utils
from db_storage import db_retrieval
from db_storage import db_storage
from scraper import request_logger
import itertools
from collections import OrderedDict

from typing import List, Set

def general_scraper(fillable_api_request: str, data_name: str, primary_keys: List[str], ignore_keys: Set):
    """
    Scrapes for all combinations denoted by a "fillable" api_request.

    Ex. http://stats.nba.com/stats/leaguedashplayerstats?...&Season={season}&SeasonSegment=

    In this case, {season} is fillable and this function will scrape
    for all seasons defined in the scraper_config.yaml file.

    Supported fillables include:


    To be supported fillables include:
    - season
    - to_date (in which case, a season will have to be fillable)
    - position
    - player_id
    - team_id
    """

    fillables = []
    fillable_types = []
    if '{season}' in fillable_api_request:
        fillables.append(SCRAPER_CONFIG.SEASONS)
        fillable_types.append('season')
        primary_keys.append('SEASON')

    if '{player_id}' in fillable_api_request:
        fillables.append(db_retrieval.fetch_player_ids())
        fillable_types.append('player_id')


    for fillable_permutation in itertools.product(*fillables):
        d = OrderedDict()
        for fillable_type, fillable_value in zip(fillable_types, fillable_permutation):
            d[fillable_type] = fillable_value

        api_request = fillable_api_request.format(**d)

        if SCRAPER_CONFIG.VERBOSE:
            print('Scraping: {}'.format(d))
            print(api_request)

        nba_response = scraper_utils.scrape(api_request)
        request_logger.log_request(api_request)
        if 'season' in fillable_types:
            nba_response.add_season_col(d['season'])
        db_storage.store_nba_response(data_name, nba_response, primary_keys, ignore_keys)



