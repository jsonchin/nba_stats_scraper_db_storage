"""
Contains functions to retrieve fillable values for
API request jobs.
"""
import datetime
from .. import db, CONFIG
from ..scrape.utils import get_date_before, format_date_for_api_request, PROPER_DATE_FORMAT


QUERY_PARAM_VALUES = {}

def get_possible_query_param_values(query_param, is_daily=False):
    """
    Valid query parameters are:
    - {SEASON}
    - {PLAYER_POSITION}

    - {GAME_ID}
    - {PLAYER_ID}
    - {GAME_DATE}
    - {DATE_TO}

    The last four return a dictionary mapping season to possible values.

    All other query parameters return a list of values to iterate through.
    """
    if is_daily:
        if query_param == '{SEASON}':
            return [CONFIG['CURRENT_SEASON']]
        elif '{DATE_TO}':
            today_date = datetime.datetime.today().strftime(PROPER_DATE_FORMAT)
            return {CONFIG['CURRENT_SEASON']: [format_date_for_api_request(get_date_before(today_date))]}

    if query_param not in QUERY_PARAM_VALUES:
        if query_param == '{SEASON}':
            values = CONFIG['SEASONS']
        elif query_param == '{PLAYER_ID}':
            values = db.retrieve.fetch_player_ids()
        elif query_param == '{GAME_DATE}':
            values = db.retrieve.fetch_game_dates()
        elif query_param == '{DATE_TO}':
            values = db.retrieve.fetch_game_dates()
            for season in values:
                for i in range(len(values[season])):
                    game_date = values[season][i]
                    date_before = get_date_before(game_date)
                    values[season][i] = format_date_for_api_request(date_before)
        elif query_param == '{GAME_ID}':
            values = db.retrieve.fetch_game_ids()
        elif query_param == '{PLAYER_POSITION}':
            values = ['G', 'F', 'C']
        else:
            raise ValueError(
                'Unsupported fillable type: {}'.format(query_param))
        QUERY_PARAM_VALUES[query_param] = values

    return QUERY_PARAM_VALUES[query_param]
