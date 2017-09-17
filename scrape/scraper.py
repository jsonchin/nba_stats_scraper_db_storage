"""
Defines common scraper patterns to be reused.

Ex. Scrape all ___ for each season
Ex. Scrape all ___ for each season for each position
Ex. Scrape all ___ for each season for each player_id
"""

import db.retrieve
import db.store
import db.request_logger
import scrape.config as SCRAPER_CONFIG

from collections import OrderedDict

import requests
import yaml
import itertools
import time
import pprint

from typing import Dict, List


class NBAResponse():
    """
    Represents a json response from stats.nba.com.
    """

    headers_access = lambda json: json['resultSets'][0]['headers']
    row_set_access = lambda json: json['resultSets'][0]['rowSet']

    def __init__(self, json_response: Dict):
        # corresponding to how NBA json responses are formatted
        try:
            self._headers = NBAResponse.headers_access(json_response)
            self._rows = NBAResponse.row_set_access(json_response)
        except ValueError:
            raise ValueError('Unexpected JSON formatting of headers and rows.')

    @property
    def headers(self):
        return self._headers

    @property
    def rows(self):
        return self._rows

    def add_season_col(self, season):
        self._headers.append('SEASON')
        for row in self.rows:
            row.append(season)

    def __str__(self):
        return '{} rows with headers: {}'.format(len(self.rows), self.headers)


def run_scrape(path_to_api_requests: str):
    """
    Runs all of the scrapes specified at the given path.
    """
    with open(path_to_api_requests, 'r') as f:
        l_requests = yaml.load(f)
        for api_request in l_requests:
            print('Running the current request:')
            pprint.pprint(api_request, indent=2)
            ignore_keys = api_request['IGNORE_KEYS'] if 'IGNORE_KEYS' in api_request else set()
            general_scraper(api_request['API_ENDPOINT'],
                                    api_request['DATA_NAME'],
                                    api_request['PRIMARY_KEYS'],
                                    ignore_keys)

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
        player_ids_by_season = db.retrieve.fetch_player_ids()
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
        player_ids_by_season = db.retrieve.fetch_player_ids()
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

        nba_response = scrape(api_request)
        db.request_logger.log_request(api_request)
        if 'season' in fillable_types:
            nba_response.add_season_col(d['season'])
        db.store.store_nba_response(data_name, nba_response, primary_keys, ignore_keys)


def scrape(api_request):
    """
    Tries to make an api_request to stats.nba.com multiple times and
    returns a NBAResponse object containing rows and headers.
    """

    def scrape_json(api_request):
        """
        Makes an api_request.
        """
        response = requests.get(url=api_request, headers={'User-agent': 'not-a-bot'})
        return response.json()

    try_count = SCRAPER_CONFIG.TRY_COUNT
    while try_count > 0:
        try:
            response_json = scrape_json(api_request)
            return NBAResponse(response_json)
        except:
            time.sleep(SCRAPER_CONFIG.SLEEP_TIME)
            print('Sleeping on {} for {} seconds.'.format(api_request, SLEEP_TIME))
        try_count -= 1
    raise IOError('Wasn\'t able to make the following request: {}'.format(api_request))


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

