"""
Defines the initial data needed to scrape any type of data.

These include:
- player_ids
- game_dates for each season
"""

from scraper.scraper_utils import scrape
import scraper.scraper_config as SCRAPER_CONFIG

def scrape_player_ids():
    """
    Scrapes and stores player_ids.
    """
    l_nba_responses = []
    for season in SCRAPER_CONFIG.SEASONS:
        PLAYER_ID_API_REQUEST = "http://stats.nba.com/stats/leaguedashplayerstats?College=&Conference=&Country=&DateFrom=&DateTo=&Division=&DraftPick=&DraftYear=&GameScope=&GameSegment=&Height=&LastNGames=0&LeagueID=00&Location=&MeasureType=Base&Month=0&OpponentTeamID=0&Outcome=&PORound=0&PaceAdjust=N&PerMode=PerGame&Period=0&PlayerExperience=&PlayerPosition=&PlusMinus=N&Rank=N&Season={season}&SeasonSegment=&SeasonType=Regular+Season&ShotClockRange=&StarterBench=&TeamID=0&VsConference=&VsDivision=&Weight=".format(season=season)
        nba_response = scrape(PLAYER_ID_API_REQUEST)
        l_nba_responses.append(nba_response)

    # TODO store into db

def scrape_game_dates():
    """
    Scrapes and stores game_dates.
    :return:
    """


if __name__ == '__main__':
    scrape_player_ids()