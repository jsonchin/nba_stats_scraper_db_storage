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

import urllib.parse
import requests
import yaml
import itertools
import time
import pprint
from scrape.utils import *

from typing import Dict, List


SEASON_DEPENDENT_FILLABLES = ['{PLAYER_ID}', '{GAME_DATE}', '{DATE_TO}']

DATE_QUERY_PARAMS = {'DATE_TO'}

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
            ignore_keys = set(api_request['IGNORE_KEYS']) if 'IGNORE_KEYS' in api_request else set()
            general_scraper(api_request['API_ENDPOINT'],
                            api_request['DATA_NAME'],
                            api_request['PRIMARY_KEYS'],
                            ignore_keys)


def run_daily_scrapes(path_to_api_requests: str):
    """
    Runs all of the daily scrape jobs specified in the
    yaml file at the given path.
    """
    with open(path_to_api_requests, 'r') as f:
        l_requests = yaml.load(f)
        for api_request in l_requests:
            if api_request['DAILY_SCRAPE']:
                print('Running the current request:')
                pprint.pprint(api_request, indent=2)
                ignore_keys = set(api_request['IGNORE_KEYS']) if 'IGNORE_KEYS' in api_request else set()
                general_scraper(api_request['API_ENDPOINT'],
                                api_request['DATA_NAME'],
                                api_request['PRIMARY_KEYS'],
                                ignore_keys)


class NBAResponse():
    """
    Represents a json response from stats.nba.com.
    """

    def __init__(self, json_response: Dict):
        # corresponding to how NBA json responses are formatted
        def access_headers(json):
            return json['resultSets'][0]['headers']

        def access_row_set(json):
            return json['resultSets'][0]['rowSet']

        try:
            self._headers = [header.upper() for header in access_headers(json_response)]
            self._rows = access_row_set(json_response)

            indicies_to_remove = set()
            for i in range(len(self.headers)):
                if self.headers[i] in SCRAPER_CONFIG.GLOBAL_IGNORE_KEYS:
                    indicies_to_remove.add(i)

            if len(indicies_to_remove) > 0:
                self._headers = [self.headers[i] for i in range(len(self.headers)) if i not in indicies_to_remove]

                for r in range(len(self.rows)):
                    row = self.rows[r]
                    self.rows[r] = [row[i] for i in range(len(row)) if i not in indicies_to_remove]

            # Other choice of the date format given in a response is OCT 29, 2016
            if 'GAME_DATE' in self.headers:
                i = self.headers.index('GAME_DATE')
                example_date = self.rows[0][i]
                if not is_proper_date_format(example_date):
                    for r in range(len(self.rows)):
                        self.rows[r][i] = format_date(self.rows[r][i])

        except ValueError:
            raise ValueError('Unexpected JSON formatting of headers and rows.')

    @property
    def headers(self):
        return self._headers

    @property
    def rows(self):
        return self._rows

    def add_col(self, col_name, value):
        self._headers.append(col_name)
        for row in self.rows:
            row.append(value)

    def __str__(self):
        return '{} rows with headers: {}'.format(len(self.rows), self.headers)


class FillableAPIRequest():
    """
    Represents a fillable api request.
    """
    fillable_values = {}
    SEASON_KEYWORD = '&Season='
    SEASON_LENGTH = len('2017-18')


    class APIRequest():

        DATE_FROM_KEY = 'DateFrom'

        def __init__(self, api_request: str, fillable_mapping: dict):
            """
            Nulls out the DateFrom query parameter.
            """
            self.fillable_mapping = fillable_mapping

            self.url_parse_components = list(urllib.parse.urlparse(api_request))
            unordered_query_params = urllib.parse.parse_qs(self.url_parse_components[4], keep_blank_values=True)
            self.query_params = OrderedDict(sorted(unordered_query_params.items(), key=lambda x: x[0]))

        def get_season(self):
            return self.get_query_param('Season')

        def get_query_param(self, param_key):
            # stats.nba query params are of title format
            param_key = format_nba_query_param(param_key)

            if param_key in self.query_params:
                if type(self.query_params[param_key]) == list and len(self.query_params[param_key]) == 1:
                    return self.query_params[param_key][0]
                else:
                    return self.query_params[param_key]
            else:
                return None

        def get_api_request_str(self, date_from=''):
            self.query_params[FillableAPIRequest.APIRequest.DATE_FROM_KEY] = [date_from]
            query_params_str = urllib.parse.urlencode(self.query_params, doseq=True)
            self.url_parse_components[4] = query_params_str
            return urllib.parse.urlunparse(self.url_parse_components)

        def __str__(self):
            return 'API Request: {}\n with fillable values: {}'.format(
                self.get_api_request_str(), self.fillable_mapping)


    def __init__(self, fillable_api_request: str, primary_keys: List[str]):
        """
        Given a fillable_api_request, parses the fillable choices
        and adds any primary keys if necesssary.
        """
        self.fillable_api_request = fillable_api_request

        self._fillable_choices = []
        self._fillable_names = []
        self._parse_fillable_api_request(primary_keys)


    def generate_api_requests(self):
        """
        Yields APIRequest objects that are generated
        by creating every combination of fillable choices.
        """
        for fillable_permutation in itertools.product(*self._fillable_choices):
            fill_mapping = OrderedDict()
            for fillable_type, fillable_value in zip(self._fillable_names, flatten_list(fillable_permutation)):
                fill_mapping[fillable_type] = fillable_value
            yield FillableAPIRequest.APIRequest(self.fill_in(**fill_mapping), fill_mapping)


    def fill_in(self, **kwargs):
        return self.fillable_api_request.format(**kwargs)


    def __str__(self):
        return 'Fillable API Request: {}\n\t with fillables: {}'.format(self.fillable_api_request, self._fillable_names)


    def _parse_fillable_api_request(self, primary_keys: List[str]):
        """
        Parses a fillable api request string by looking for
        specific keywords in the string such as '{SEASON}'
        which denotes that the job should scrape for all
        seasons.
        """
        if '{SEASON}' in self.fillable_api_request:
            self._fillable_names.append('SEASON')

            for dependent_fillable in SEASON_DEPENDENT_FILLABLES:
                if dependent_fillable in self.fillable_api_request:
                    self._fillable_names.append(dependent_fillable[1:-1])

            grouped_choices = []
            for season in FillableAPIRequest._get_fillable_values('{SEASON}'):
                fillable_values = []

                for dependent_fillable in SEASON_DEPENDENT_FILLABLES:
                    if dependent_fillable in self.fillable_api_request:
                        fillable_values.append(FillableAPIRequest._get_fillable_values(dependent_fillable)[season])

                for grouped_choice in itertools.product(*fillable_values):
                    grouped_choice = [season] + list(grouped_choice)
                    grouped_choices.append(grouped_choice)

            self._fillable_choices.append(grouped_choices)

        elif '{PLAYER_ID}' in self.fillable_api_request:
            player_ids_by_season = FillableAPIRequest._get_fillable_values('{PLAYER_ID}')
            self._fillable_names.append('PLAYER_ID')

            # find what the season is in the api request
            try:
                i = self.fillable_api_request.index(FillableAPIRequest.SEASON_KEYWORD)
                season_start_i = i + len(FillableAPIRequest.SEASON_KEYWORD)
                season = self.fillable_api_request[season_start_i: season_start_i + FillableAPIRequest.SEASON_LENGTH]
                self._fillable_choices.append(player_ids_by_season[season])
            except ValueError:
                raise ValueError('API request had {PLAYER_ID} without a specified season or {SEASON}.')


    @staticmethod
    def _get_fillable_values(fillable_type):
        if fillable_type not in FillableAPIRequest.fillable_values:
            if fillable_type == '{SEASON}':
                values = SCRAPER_CONFIG.SEASONS
            elif fillable_type == '{PLAYER_ID}':
                values = db.retrieve.fetch_player_ids()
            elif fillable_type == '{GAME_DATE}':
                values = db.retrieve.fetch_game_dates()
            elif fillable_type == '{DATE_TO}':
                values = db.retrieve.fetch_game_dates(day_before=True, format_api_request=True)
            else:
                raise ValueError('Unsupported fillable type: {}'.format(fillable_type))
            FillableAPIRequest.fillable_values[fillable_type] = values
            return values
        else:
            return FillableAPIRequest.fillable_values[fillable_type]


def minimize_api_scrape(api_request: FillableAPIRequest.APIRequest):
    """
    Adds or updates the "DateFrom" api query parameter
    based on the last time this api_request was made.
    """
    api_request_without_datefrom = api_request.get_api_request_str()

    if db.request_logger.already_scraped(api_request_without_datefrom):
        date_str = db.request_logger.get_last_scraped(api_request_without_datefrom)
        date_str = date_str[:len(EXAMPLE_PROPER_DATE)]
        date_from = (datetime.datetime.strptime(date_str, PROPER_DATE_FORMAT) - datetime.timedelta(days=2))\
            .strftime(PROPER_DATE_FORMAT)
        return api_request.get_api_request_str(date_from)
    else:
        return api_request.get_api_request_str()


def general_scraper(fillable_api_request_str: str, data_name: str, primary_keys: List[str], ignore_keys=set()):
    """
    Scrapes for all combinations denoted by a "fillable" api_request.

    Ex. http://stats.nba.com/stats/leaguedashplayerstats?...&Season={SEASON}&SeasonSegment=

    In this case, {SEASON} is fillable and this function will scrape
    for all seasons defined in the scraper_config.yaml file.

    Supported fillables include:
    - season
    - player_id

    To be supported fillables include:
    - to_date (in which case, a season will have to be fillable)
    - position
    - team_id
    """

    fillable_api_request = FillableAPIRequest(fillable_api_request_str, primary_keys)
    print(fillable_api_request)

    for api_request in fillable_api_request.generate_api_requests():

        if db.request_logger.already_scraped(api_request.get_api_request_str()):
            if api_request.get_season() == SCRAPER_CONFIG.CURRENT_SEASON:
                if SCRAPER_CONFIG.MINIMIZE_SCRAPES:
                    api_request_str = minimize_api_scrape(api_request)
                else:
                    api_request_str = api_request.get_api_request_str()
            else:
                print('Skipping api_request: {}\n because it has already been scraped.'.format(api_request))
                continue
        else:
            api_request_str = api_request.get_api_request_str()

        if SCRAPER_CONFIG.VERBOSE:
            print(api_request)

        nba_response = scrape(api_request_str)

        for key in primary_keys:
            key = format_str_to_nba_response_header(key)

            if key not in nba_response.headers:
                col_val = api_request.get_query_param(key)

                if key in DATE_QUERY_PARAMS and not is_proper_date_format(col_val):
                    col_val = format_date(col_val)

                if col_val is not None:
                    nba_response.add_col(key, col_val)
                else:
                    raise ValueError('Unexpected primary key: {}'.format(key))

        db.store.store_nba_response(data_name, nba_response, primary_keys, ignore_keys)

        # log after it has been stored
        db.request_logger.log_request(api_request.get_api_request_str())


def scrape(api_request):
    """
    Tries to make an api_request to stats.nba.com multiple times and
    returns a NBAResponse object containing rows and headers.
    """

    def scrape_json(api_request):
        """
        Makes an api_request.
        """
        USER_AGENT = 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/60.0.3112.101 Safari/537.36'
        response = requests.get(url=api_request, headers={'User-agent':USER_AGENT},
                                stream=True, allow_redirects=False)
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
