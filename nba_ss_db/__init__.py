import yaml
import os


def load_config(file_name):
    """
    Loads the configurations located in the config file
    at config/config.yaml
    into the CONFIG dictionary.
    """
    CONFIG = {}
    with open(file_name, 'r') as f:
        config_yaml = yaml.load(f)
        for config_key in config_yaml:
            CONFIG[config_key] = config_yaml[config_key]
    return CONFIG


__location__ = os.path.realpath(os.path.join(
    os.getcwd(), os.path.dirname(__file__)))
CONFIG = load_config(os.path.join(__location__, 'config.yaml'))


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
