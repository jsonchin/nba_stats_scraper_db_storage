import datetime


PROPER_DATE_FORMAT = '%Y-%m-%d'
EXAMPLE_PROPER_DATE = '2016-10-29'


def format_nba_query_param(s: str):
    """
    stats.nba request query parameters are in UpperCamelCase
    format with the exception of Id which is ID.
    """
    return ''.join([w.title() if w.lower() != 'id' else 'ID' for w in s.split('_')])


def format_str_to_nba_response_header(s: str):
    """
    Formats a string into NBA response column header format.

    stats.nba response columns are made into ANGRY_SNAKE_CASE
    format when they are parsed from json.

    >>> format_str_to_nba_response_header('game_date')
    'GAME_DATE'
    >>> format_str_to_nba_response_header('player_id')
    'PLAYER_ID'
    >>> format_str_to_nba_response_header('GAME_ID')
    'GAME_ID'
    >>> format_str_to_nba_response_header('Season')
    'SEASON'
    """
    return s.upper()


def is_proper_date_format(date_str):
    """
    Returns True if date_str is in YYYY-MM-DD format.

    >>> is_proper_date_format('2016-10-29')
    True
    >>> is_proper_date_format('OCT 29, 2016')
    False
    >>> is_proper_date_format('2016-10-29T000001')
    False
    """
    try:
        datetime.datetime.strptime(date_str, PROPER_DATE_FORMAT)
        return True
    except:
        return False


def format_date(date_str):
    """
    Formats the date_str into YYYY-MM-DD format.

    Throws an exception if the date format was unsupported

    Add translations as they show up:
    Currently supported:
    OCT 29, 2016
    YYYY-MM-DD[extra_chars]

    >>> format_date('OCT 29, 2016')
    '2016-10-29'
    >>> format_date('2016-10-29T000001')
    '2016-10-29'

    """
    # OCT 29, 2016
    try:
        return datetime.datetime.strftime(
            datetime.datetime.strptime(date_str, '%b %d, %Y'),
            PROPER_DATE_FORMAT
        )
    except:
        pass

    # YYYY-MM-DD[extra_chars]
    return datetime.datetime.strftime(
        datetime.datetime.strptime(date_str[:len(EXAMPLE_PROPER_DATE)], PROPER_DATE_FORMAT),
        PROPER_DATE_FORMAT
    )


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
