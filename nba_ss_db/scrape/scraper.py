"""
Defines common scraper patterns to be reused.

Ex. Scrape all ___ for each season
Ex. Scrape all ___ for each season for each position
Ex. Scrape all ___ for each season for each player_id
"""
from typing import Dict, List
import time
import pprint
import requests
import yaml

from .. import db, CONFIG
from .fillable_api_request import FillableAPIRequest
from .utils import format_str_to_nba_response_header


def run_scrape_jobs(path_to_api_requests: str, is_daily=False):
    """
    Runs all of the scrape jobs specified in the
    yaml file at the given path.
    """
    with open(path_to_api_requests, 'r') as f:
        l_requests = yaml.load(f)
        for api_request in l_requests:
            if CONFIG['VERBOSE']:
                print('Running the current request:')
                pprint.pprint(api_request, indent=2)
            overwrite_scrape = api_request['DAILY_SCRAPE'] and is_daily
            ignore_keys = set(api_request['IGNORE_KEYS']) if 'IGNORE_KEYS' in api_request else set()
            result_set_index = 0 if 'RESULT_SET_INDEX' not in api_request else api_request['RESULT_SET_INDEX']
            general_scraper(fillable_api_request_str=api_request['API_ENDPOINT'],
                            data_name=api_request['DATA_NAME'],
                            primary_keys=api_request['PRIMARY_KEYS'],
                            result_set_index=result_set_index,
                            ignore_keys=ignore_keys,
                            overwrite=overwrite_scrape,
                            is_daily=is_daily)


def run_daily_scrapes(path_to_api_requests: str):
    """
    Runs all of the daily scrape jobs specified in the
    yaml file at the given path.
    """
    run_scrape_jobs(path_to_api_requests, is_daily=True)


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


def general_scraper(fillable_api_request_str: str, data_name: str, primary_keys: List[str],
                    result_set_index, ignore_keys=set(), overwrite=False,
                    is_daily=False):
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
    fillable_api_request = FillableAPIRequest(fillable_api_request_str, is_daily)
    if CONFIG['VERBOSE']:
        print(fillable_api_request)

    for api_request in fillable_api_request.generate_api_requests():

        if not (overwrite or not db.request_logger.already_scraped(api_request.api_request)):
            if CONFIG['VERBOSE']:
                print('Skipping api_request: {}\n because it has already been scraped.'.format(api_request))
            continue

        print(api_request)

        api_request_str = api_request.api_request
        nba_response = scrape(api_request_str, result_set_index)

        # add any primary key columns from query params
        for key in primary_keys:
            key = format_str_to_nba_response_header(key)

            # add any primary keys not provided in the response
            if key not in nba_response.headers:
                col_val = api_request.query_params[key]

                if col_val is not None:
                    nba_response.add_col(key, col_val)
                else:
                    raise ValueError('Unexpected primary key: {}'.format(key))

        # store the response into the db
        db.store.store_nba_response(data_name, nba_response, primary_keys, ignore_keys)

        # log after it has been stored
        db.request_logger.log_request(api_request_str, data_name)


def scrape(api_request, result_set_index):
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
            return NBAResponse(response_json, result_set_index)
        except:
            time.sleep(CONFIG['SLEEP_TIME'])
            print('Sleeping on {} for {} seconds.'.format(api_request, CONFIG['SLEEP_TIME']))
        try_count -= 1
    raise IOError('Wasn\'t able to make the following request: {}'.format(api_request))
