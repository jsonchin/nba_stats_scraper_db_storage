START_YEAR = 2015
END_YEAR = 2017
TRY_COUNT = 5   # amount of times to try to make an api request
SLEEP_TIME = 2  # in seconds (time to wait after a failed request
VERBOSE = True
MINIMIZE_SCRAPES = True # scrapes from when it last scraped
INITIAL_DATE_FROM = '2009-08-17'


def _get_seasons():
    """
    Returns a list of strings representing seasons in the format:
    '20XX-YY'
    """
    start_year = START_YEAR
    end_year = END_YEAR

    seasons = []
    while start_year != end_year:
        seasons.append('{}-{}'.format(start_year, (start_year + 1) % 100))
        start_year += 1
    return seasons

SEASONS = _get_seasons()