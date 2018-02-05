import unittest
from tests.test_setup import init_test_db

from nba_ss_db import db
from nba_ss_db import scrape


class TestDBStorage(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        pass

    @classmethod
    def tearDownClass(cls):
        pass

    @classmethod
    def setUp(cls):
        init_test_db()

    def test_db_init(self):
        """
        Tests if init_db creates a table called 'scrape_log'.
        """
        db.initialize.init_db()
        self.assertTrue(db.retrieve.exists_table('scrape_log'), 'Table \'scrape_log\' should exist.')


    def test_exists_db(self):
        """
        Tests that db.retrieve.exists_table properly
        detects if a table exists or not.
        """
        self.assertFalse(db.retrieve.exists_table('TABLE_NAME_THAT_DOES_NOT_EXIST'),
                         'Table should not exist.')
        self.assertTrue(db.retrieve.exists_table('player_ids'))
    
    
    def test_db_store(self):
        """
        Tests store_nba_responses in db/store.
        """
        nba_responses = []
        scraped_json = {
            'resource': "leaguegamelog",
            'parameters': {
                'LeagueID': "00",
                'Season': "2017-18",
                'SeasonType': "Regular Season",
                'PlayerOrTeam': "T",
                'Counter': 1000,
                'Sorter': "DATE",
                'Direction': "DESC",
                'DateFrom': "02/04/2018",
                'DateTo': None
            },
            'resultSets': [
                {
                    'name': "LeagueGameLog",
                    'headers': ['SEASON_ID', 'TEAM_ID', 'TEAM_ABBREVIATION', 'TEAM_NAME', 'GAME_ID', 'GAME_DATE', 'MATCHUP', 'WL', 'MIN', 'FGM', 'FGA', 'FG_PCT', 'FG3M', 'FG3A', 'FG3_PCT', 'FTM', 'FTA', 'FT_PCT', 'OREB', 'DREB', 'REB', 'AST', 'STL', 'BLK', 'TOV', 'PF', 'PTS', 'PLUS_MINUS', 'VIDEO_AVAILABLE'],
                    'rowSet': [
                        ['22017', 1610612756, 'PHX', 'Phoenix Suns', '0021700789', '2018-02-04', 'PHX vs. CHA', 'L', 240, 41, 77, 0.532, 15, 29, 0.517, 13, 13, 1, 2, 37, 39, 24, 4, 4, 19, 25, 110, -5, 1],
                        ['22017', 1610612766, 'CHA', 'Charlotte Hornets', '0021700789', '2018-02-04', 'CHA @ PHX', 'W', 240, 40, 96, 0.417, 10, 38, 0.263, 25, 29, 0.862, 13, 30, 43, 24, 12, 6, 8, 17, 115, 5, 1]
                    ]
                }
            ]
        }
        primary_keys = ('SEASON_ID', 'TEAM_ID', 'GAME_DATE')
        nba_response = scrape.scraper.NBAResponse(scraped_json, 0)
        nba_responses.append(nba_response)
        table_name = 'team_logs_test'
        db.store.store_nba_responses(table_name, nba_responses, primary_keys=primary_keys)
        self.assertTrue(db.retrieve.exists_table(table_name))
        num_rows_in_table = len(db.retrieve.db_query("""SELECT * FROM {};""".format(table_name)))
        self.assertEquals(num_rows_in_table, len(scraped_json['resultSets'][0]['rowSet']))
