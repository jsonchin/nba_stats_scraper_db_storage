START_YEAR = 2015
END_YEAR = 2017

def _get_seasons():
    """
    Returns a list of strings representing seasons in the format:
    '20XX-YY'
    """
    start_year = START_YEAR
    end_year = END_YEAR

    seasons = []
    while start_year != end_year - 1:
        seasons.append('{}-{}'.format(start_year, (start_year + 1) % 100))
        start_year += 1
    return seasons

SEASONS = _get_seasons()