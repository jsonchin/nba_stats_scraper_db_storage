import db.initialize
import scrape.initialize
import scrape.scraper
import db.utils

if __name__ == '__main__':
    db.initialize.init_db()
    # scraper_init.scrape_player_ids()
    # scraper.general_scraper('http://stats.nba.com/stats/playergamelog?LeagueID=00&PlayerID={player_id}&Season={season}&SeasonType=Regular+Season',
    #                               'player_logs',
    #                         ['GAME_DATE', 'PLAYER_ID'])

    scrape.scraper.run_scrape('api_requests.yaml')
    db.utils.close_con()
