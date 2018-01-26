"""
Defines common scraper patterns to be reused.

Ex. Scrape all ___ for each season
Ex. Scrape all ___ for each season for each position
Ex. Scrape all ___ for each season for each player_id
"""
from .. import db, CONFIG

from collections import OrderedDict

import urllib.parse
import requests
import yaml
import itertools
import time
import pprint
from ..scrape.utils import *
import query_param_values
from .fillable_api_request import FillableAPIRequest

from typing import Dict, List

VALID_FILLABLES = {'{SEASON}', '{PLAYER_ID}', '{GAME_DATE}', '{DATE_TO}', '{GAME_ID}', '{PLAYER_POSITION}'}

SEASON_DEPENDENT_FILLABLES = ['{PLAYER_ID}', '{GAME_DATE}', '{DATE_TO}', '{GAME_ID}']

OTHER_FILLABLES = VALID_FILLABLES - set(SEASON_DEPENDENT_FILLABLES) - {'{SEASON}'}

DATE_QUERY_PARAMS = {'DATE_TO'}

RUN_DAILY = False

def run_scrape_jobs(path_to_api_requests: str):
    """
    Runs all of the scrape jobs specified in the
    yaml file at the given path.
    """
    global RUN_DAILY
    RUN_DAILY = False

    with open(path_to_api_requests, 'r') as f:
        l_requests = yaml.load(f)
        for api_request in l_requests:
            if CONFIG['VERBOSE']:
                print('Running the current request:')
                pprint.pprint(api_request, indent=2)
            ignore_keys = set(api_request['IGNORE_KEYS']) if 'IGNORE_KEYS' in api_request else set()
            resultSetIndex = 0 if 'RESULT_SET_INDEX' not in api_request else api_request['RESULT_SET_INDEX']
            general_scraper(api_request['API_ENDPOINT'],
                            api_request['DATA_NAME'],
                            api_request['PRIMARY_KEYS'],
                            resultSetIndex,
                            ignore_keys)


def run_daily_scrapes(path_to_api_requests: str):
    """
    Runs all of the daily scrape jobs specified in the
    yaml file at the given path.
    """
    global RUN_DAILY
    RUN_DAILY = True
    
    with open(path_to_api_requests, 'r') as f:
        l_requests = yaml.load(f)
        for api_request in l_requests:
            overwrite_scrape = api_request['DAILY_SCRAPE']
            print('Running the current request:')
            pprint.pprint(api_request, indent=2)
            ignore_keys = set(api_request['IGNORE_KEYS']) if 'IGNORE_KEYS' in api_request else set()
            resultSetIndex = 0 if 'RESULT_SET_INDEX' not in api_request else api_request['RESULT_SET_INDEX']
            general_scraper(api_request['API_ENDPOINT'],
                            api_request['DATA_NAME'],
                            api_request['PRIMARY_KEYS'],
                            resultSetIndex,
                            ignore_keys=ignore_keys,
                            overwrite=overwrite_scrape)


class NBAResponse():
    """
    Represents a json response from stats.nba.com.
    """

    def __init__(self, json_response: Dict, resultSetIndex: int):
        # corresponding to how NBA json responses are formatted
        def access_headers(json):
            return json['resultSets'][resultSetIndex]['headers']

        def access_row_set(json):
            return json['resultSets'][resultSetIndex]['rowSet']

        try:
            self._headers = [header.upper() for header in access_headers(json_response)]
            self._rows = access_row_set(json_response)

            indicies_to_remove = set()
            for i in range(len(self.headers)):
                if self.headers[i] in CONFIG['GLOBAL_IGNORE_KEYS']:
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


def general_scraper(fillable_api_request_str: str, data_name: str, primary_keys: List[str], resultSetIndex, ignore_keys=set(), overwrite=False):
    """
    Scrapes for all combinations denoted by a "fillable" api_request.

    Ex. http://stats.nba.com/stats/leaguedashplayerstats?...&Season={SEASON}&SeasonSegment=

    In this case, {SEASON} is fillable and this function will scrape
    for all seasons defined in the config.yaml file.

    Supported fillables include:
    - season
    - player_id

    To be supported fillables include:
    - to_date (in which case, a season will have to be fillable)
    - position
    - team_id
    """

    print(data_name)
    fillable_api_request = FillableAPIRequest(fillable_api_request_str)
    if CONFIG['VERBOSE']:
        print(fillable_api_request)

    for api_request in fillable_api_request.generate_api_requests():

        if not (overwrite or not db.request_logger.already_scraped(api_request.api_request)):
            if CONFIG['VERBOSE']:
                print('Skipping api_request: {}\n because it has already been scraped.'.format(api_request))
            continue

        print(api_request)

        api_request_str = api_request.api_request
        nba_response = scrape(api_request_str, resultSetIndex)

        # format the nba_response before storing it
        for key in primary_keys:
            key = format_str_to_nba_response_header(key)

            # add any primary keys not provided in the response
            if key not in nba_response.headers:
                col_val = api_request.query_params[key]

                # format dates
                if key in DATE_QUERY_PARAMS and not is_proper_date_format(col_val):
                    col_val = format_date(col_val)

                if col_val is not None:
                    nba_response.add_col(key, col_val)
                else:
                    raise ValueError('Unexpected primary key: {}'.format(key))

        # store the response into the db
        db.store.store_nba_response(data_name, nba_response, primary_keys, ignore_keys)

        # log after it has been stored
        db.request_logger.log_request(api_request_str, data_name)


def scrape(api_request, resultSetIndex):
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

    try_count = CONFIG['TRY_COUNT']
    while try_count > 0:
        try:
            response_json = scrape_json(api_request)
            return NBAResponse(response_json, resultSetIndex)
        except:
            time.sleep(CONFIG['SLEEP_TIME'])
            print('Sleeping on {} for {} seconds.'.format(api_request, CONFIG['SLEEP_TIME']))
        try_count -= 1
    raise IOError('Wasn\'t able to make the following request: {}'.format(api_request))
