from db import initialize
from scrape import initialize
from scrape import scraper
from db import utils
from db import retrieve

if __name__ == '__main__':
    initialize.init_db()
    # scraper_init.scrape_player_ids()
    scraper.general_scraper('http://stats.nba.com/stats/playergamelog?LeagueID=00&PlayerID={player_id}&Season={season}&SeasonType=Regular+Season',
                                  'player_logs',
                            ['GAME_DATE', 'PLAYER_ID'])

    utils.close_con()
