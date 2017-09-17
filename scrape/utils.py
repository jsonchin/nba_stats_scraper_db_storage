"""
Provides utility functions for scraping.
"""
from scrape.nba_response import NBA_response
import requests
import time


def scrape(api_request):

    def scrape_json(api_request):
        try_count = 5
        SLEEP_TIME = 2 # in seconds
        while try_count > 0:
            try:
                response = requests.get(url=api_request, headers={'User-agent': 'not-a-bot'})
                return response
            except:
                time.sleep(SLEEP_TIME)
                print('Sleeping on {} for {} seconds.'.format(api_request, SLEEP_TIME))
            try_count -= 1
        raise IOError('Wasn\'t able to make the following request: {}'.format(api_request))

    response = scrape_json(api_request)
    response_json = response.json()

    try:
        return NBA_response(response_json)
    except:
        raise ValueError('Unexpected JSON formatting of headers and rows.')

