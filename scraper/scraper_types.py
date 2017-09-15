"""
Defines common scraper patterns to be reused.

Ex. Scrape all ___ for each season
Ex. Scrape all ___ for each season for each position
Ex. Scrape all ___ for each season for each player_id
"""

def general_scraper(fillable_api_request: str):
    """
    Scrapes for all combinations denoted by a "fillable" api_request.

    Ex. http://stats.nba.com/stats/leaguedashplayerstats?...&Season={season}&SeasonSegment=

    In this case, {season} is fillable and this function will scrape
    for all seasons defined in the scraper_config.yaml file.

    Supported fillables include:


    To be supported fillables include:
    - season
    - to_date (in which case, a season will have to be fillable)
    - position
    - player_id
    - team_id
    """