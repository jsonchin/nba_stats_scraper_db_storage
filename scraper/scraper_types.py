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

from typing import List


def flatten_list(l):
    """
    Flattens a list one level.
    """
    flattened_l = []
    for ele in l:
        if type(ele) == list or type(ele) == tuple:
            flattened_l.extend(ele)
        else:
            flattened_l.append(ele)
    return flattened_l

def general_scraper(fillable_api_request: str, data_name: str, primary_keys: List[str], ignore_keys=set()):
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
    SEASON_KEYWORD = '&Season='
    SEASON_LENGTH = len('2017-18')

    fillables = []
    fillable_types = []

    if '{season}' in fillable_api_request and '{player_id}' in fillable_api_request:
        player_ids_by_season = db_retrieval.fetch_player_ids()
        fillable_types.append('season')
        fillable_types.append('player_id')

        paired_season_player_id = []
        for season in player_ids_by_season:
            player_ids = player_ids_by_season[season]
            for player_id in player_ids:
                paired_season_player_id.append((season, player_id))
        fillables.append(paired_season_player_id)

    elif '{season}' in fillable_api_request:
        fillable_types.append('season')
        fillables.append(SCRAPER_CONFIG.SEASONS)
        primary_keys.append('SEASON')

    elif '{player_id}' in fillable_api_request:
        player_ids_by_season = db_retrieval.fetch_player_ids()
        fillable_types.append('player_id')

        # find what the season is in the api request
        try:
            i = fillable_api_request.index(SEASON_KEYWORD)
            season_start_i = i + len(SEASON_KEYWORD)
            season = fillable_api_request[season_start_i: season_start_i + SEASON_LENGTH]
            fillables.append(player_ids_by_season[season])
        except ValueError:
            raise ValueError('API request had {player_id} without a specified season or {season}.')


    for fillable_permutation in itertools.product(*fillables):
        d = OrderedDict()

        for fillable_type, fillable_value in zip(fillable_types, flatten_list(fillable_permutation)):
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



