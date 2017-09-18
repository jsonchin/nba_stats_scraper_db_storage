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


def run_scrape_jobs(path_to_api_requests: str):
    """
    Runs all of the scrape jobs specified in the
    yaml file at the given path.
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

    def add_game_date_col(self, game_date):
        self._headers.append('GAME_DATE')
        for row in self.rows:
            row.append(game_date)

    def __str__(self):
        return '{} rows with headers: {}'.format(len(self.rows), self.headers)

class FillableAPIRequest():
    """
    Represents a fillable api request.
    """
    fillable_values = {}
    SEASON_KEYWORD = '&Season='
    SEASON_LENGTH = len('2017-18')

    def __init__(self, fillable_api_request: str, primary_keys: List[str]):
        """
        Given a fillable_api_request, parses the fillable choices
        and adds any primary keys if necesssary.
        """
        self.fillable_api_request = fillable_api_request
        self._fillable_choices = []
        self._fillable_names = []
        self._parse_fillable_api_request(primary_keys)

    def generate_cross_product_choices(self):
        for fillable_permutation in itertools.product(*self._fillable_choices):
            d = OrderedDict()
            for fillable_type, fillable_value in zip(self._fillable_names, flatten_list(fillable_permutation)):
                d[fillable_type] = fillable_value
            yield d

    def format(self, **kwargs):
        return self.fillable_api_request.format(**kwargs)

    def _parse_fillable_api_request(self, primary_keys: List[str]):
        if '{season}' in self.fillable_api_request:
            self._fillable_names.append('season')

            SEASON_DEPENDENT_FILLABLES = ['{player_id}', '{game_date}']

            for dependent_fillable in SEASON_DEPENDENT_FILLABLES:
                if dependent_fillable in self.fillable_api_request:
                    self._fillable_names.append(dependent_fillable[1:-1])


            grouped_choices = []
            for season in self._get_fillable_values('{season}'):
                fillable_values = []

                for dependent_fillable in SEASON_DEPENDENT_FILLABLES:
                    if dependent_fillable in self.fillable_api_request:
                        fillable_values.append(self._get_fillable_values(dependent_fillable)[season])

                for grouped_choice in itertools.product(*fillable_values):
                    grouped_choice = [season] + list(grouped_choice)
                    grouped_choices.append(grouped_choice)

            self._fillable_choices.append(grouped_choices)

        elif '{player_id}' in self.fillable_api_request:
            player_ids_by_season = self._get_fillable_values('{player_id}')
            self._fillable_names.append('player_id')

            # find what the season is in the api request
            try:
                i = self.fillable_api_request.index(FillableAPIRequest.SEASON_KEYWORD)
                season_start_i = i + len(FillableAPIRequest.SEASON_KEYWORD)
                season = self.fillable_api_request[season_start_i: season_start_i + FillableAPIRequest.SEASON_LENGTH]
                self._fillable_choices.append(player_ids_by_season[season])
            except ValueError:
                raise ValueError('API request had {player_id} without a specified season or {season}.')

    def __str__(self):
        return 'API Request: {}\n\t with fillable values: {}'.format(self.fillable_api_request, self._fillable_names)


    def _get_fillable_values(self, fillable_type):
        if fillable_type not in FillableAPIRequest.fillable_values:
            if fillable_type == '{season}':
                values = SCRAPER_CONFIG.SEASONS
            elif fillable_type == '{player_id}':
                values = db.retrieve.fetch_player_ids()
            elif fillable_type == '{game_date}':
                values = db.retrieve.fetch_game_dates()
            else:
                raise ValueError('Unsupported fillable type: {}'.format(fillable_type))
            FillableAPIRequest.fillable_values[fillable_type] = values
            return values
        else:
            return FillableAPIRequest.fillable_values[fillable_type]




def general_scraper(fillable_api_request_str: str, data_name: str, primary_keys: List[str], ignore_keys=set()):
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
    fillable_api_request = FillableAPIRequest(fillable_api_request_str, primary_keys)
    print(fillable_api_request)

    for fillable_choice in fillable_api_request.generate_cross_product_choices():
        api_request = fillable_api_request.format(**fillable_choice)

        if SCRAPER_CONFIG.VERBOSE:
            print('Scraping: {}'.format(fillable_choice))
            print(api_request)

        nba_response = scrape(api_request)
        db.request_logger.log_request(api_request)
        for key in primary_keys:
            if key not in nba_response.headers:
                if 'season' in fillable_choice:
                    nba_response.add_season_col(fillable_choice['season'])
                elif 'game_date' in fillable_choice:
                    nba_response.add_game_date_col(fillable_choice['game_date'])
                else:
                    raise ValueError('Unexpected primary key: {}'.format(key))
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
            print('Sleeping on {} for {} seconds.'.format(api_request, SCRAPER_CONFIG.SLEEP_TIME))
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