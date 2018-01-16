START_YEAR = 2013
END_YEAR = 2018
TRY_COUNT = 5   # amount of times to try to make an api request
SLEEP_TIME = 2  # in seconds (time to wait after a failed request
VERBOSE = False
MINIMIZE_SCRAPES = True # scrapes from when it last scraped
DAILY = False
INITIAL_DATE_FROM = '2009-08-17'
CURRENT_SEASON = '2017-18'
GLOBAL_IGNORE_KEYS = {
    'VIDEO_AVAILABLE',

    'AST_PCT_RANK', 'AST_RATIO_RANK', 'AST_TO_RANK', 'DEF_RATING_RANK', 'DREB_PCT_RANK', 'EFG_PCT_RANK', 'FGA_PG_RANK', 'FGA_RANK', 'FGM_PG_RANK', 'FGM_RANK', 'FG_PCT_RANK', 'GP_RANK', 'L_RANK', 'MIN_RANK', 'NET_RATING_RANK', 'OFF_RATING_RANK', 'OREB_PCT_RANK', 'PACE_RANK', 'PCT_AST_RANK', 'PCT_BLKA_RANK', 'PCT_BLK_RANK', 'PCT_DREB_RANK', 'PCT_FG3A_RANK', 'PCT_FG3M_RANK', 'PCT_FGA_RANK', 'PCT_FGM_RANK', 'PCT_FTA_RANK', 'PCT_FTM_RANK', 'PCT_OREB_RANK', 'PCT_PFD_RANK', 'PCT_PF_RANK', 'PCT_PTS_RANK', 'PCT_REB_RANK', 'PCT_STL_RANK', 'PCT_TOV_RANK', 'PIE_RANK', 'REB_PCT_RANK', 'TM_TOV_PCT_RANK', 'TS_PCT_RANK', 'USG_PCT_RANK', 'W_PCT_RANK', 'W_RANK'
}


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