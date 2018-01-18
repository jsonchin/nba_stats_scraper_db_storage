from config import config
CONFIG = config.load_config('config/config.yaml')


def _get_seasons():
    """
    Returns a list of strings representing seasons in the format:
    '20XX-YY'
    """
    start_year = CONFIG['START_YEAR']
    end_year = CONFIG['END_YEAR']

    seasons = []
    while start_year != end_year:
        seasons.append('{}-{}'.format(start_year, (start_year + 1) % 100))
        start_year += 1
    return seasons


SEASONS = _get_seasons()
CONFIG['SEASONS'] = SEASONS
