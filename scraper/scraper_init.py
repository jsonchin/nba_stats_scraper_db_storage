"""
Defines the initial data needed to scrape any type of data.

These include:
- player_ids
- game_dates for each season
"""

from scraper.scraper_utils import scrape
import scraper.scraper_config as SCRAPER_CONFIG
from scraper.scraper_types import general_scraper
import db_storage.db_storage as db_store

def scrape_player_ids():
    """
    Scrapes and stores player_ids.
    """
    PLAYER_ID_API_REQUEST = 'http://stats.nba.com/stats/leaguedashplayerstats?College=&Conference=&Country=&DateFrom=&DateTo=&Division=&DraftPick=&DraftYear=&GameScope=&GameSegment=&Height=&LastNGames=0&LeagueID=00&Location=&MeasureType=Base&Month=0&OpponentTeamID=0&Outcome=&PORound=0&PaceAdjust=N&PerMode=PerGame&Period=0&PlayerExperience=&PlayerPosition=&PlusMinus=N&Rank=N&Season={season}&SeasonSegment=&SeasonType=Regular+Season&ShotClockRange=&StarterBench=&TeamID=0&VsConference=&VsDivision=&Weight='
    PRIMARY_KEYS = ['PLAYER_ID', 'PLAYER_NAME']
    ignore_keys = set(["TEAM_ID","TEAM_ABBREVIATION","AGE","GP","W","L","W_PCT","MIN","FGM","FGA","FG_PCT","FG3M","FG3A","FG3_PCT","FTM","FTA","FT_PCT","OREB","DREB","REB","AST","TOV","STL","BLK","BLKA","PF","PFD","PTS","PLUS_MINUS","DD2","TD3","GP_RANK","W_RANK","L_RANK","W_PCT_RANK","MIN_RANK","FGM_RANK","FGA_RANK","FG_PCT_RANK","FG3M_RANK","FG3A_RANK","FG3_PCT_RANK","FTM_RANK","FTA_RANK","FT_PCT_RANK","OREB_RANK","DREB_RANK","REB_RANK","AST_RANK","TOV_RANK","STL_RANK","BLK_RANK","BLKA_RANK","PF_RANK","PFD_RANK","PTS_RANK","PLUS_MINUS_RANK","DD2_RANK","TD3_RANK","CFID","CFPARAMS"])
    general_scraper(PLAYER_ID_API_REQUEST, 'player_ids', PRIMARY_KEYS, ignore_keys)


def scrape_game_dates():
    """
    Scrapes and stores game_dates.
    :return:
    """