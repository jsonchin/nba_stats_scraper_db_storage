from db import db_init
from scrape import scraper_init
from scrape import scraper_types
from db import db_utils
from db import db_retrieval

if __name__ == '__main__':
    db_init.init_db()
    # scraper_init.scrape_player_ids()
    scraper_types.general_scraper('http://stats.nba.com/stats/playergamelog?LeagueID=00&PlayerID={player_id}&Season={season}&SeasonType=Regular+Season',
                                  'player_logs',
                                  ['GAME_DATE', 'PLAYER_ID'])

    db_utils.close_con()
