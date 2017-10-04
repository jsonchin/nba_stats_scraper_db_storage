import unittest
from db import config
config.DB_NAME = 'test_db'
config.DB_PATH = 'tests/db/databases'

import db.retrieve

from collections import defaultdict

class TestDBStorage(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        pass

    @classmethod
    def tearDownClass(cls):
        pass

    @classmethod
    def setUp(cls):
        pass

    def test_fetch_player_ids(self):
        """
        Tests that fetch_player_ids returns a dictionary
        with appropriate keys.
        """
        d = db.retrieve.fetch_player_ids()
        self.assertTrue(type(d) == dict or type(d) == defaultdict,
                        'Expected a dictionary got {}'.format(type(d)))

        SEASON_LENGTH = len('2017-18')
        self.assertTrue(all([len(key) == SEASON_LENGTH for key in d.keys()]))

    def test_get_table_names(self):
        """
        Tests that get_table_names excludes certain tables on default.
        """
        table_names = db.retrieve.get_table_names()
        self.assertTrue('scrape_log' not in table_names)
        self.assertTrue('player_ids' not in table_names)


    def test_get_all_table_names(self):
        """
        Tests that get_table_names excludes certain tables on flag False.
        """
        table_names = db.retrieve.get_table_names(only_data=False)
        self.assertTrue('scrape_log' in table_names)
        self.assertTrue('player_ids' in table_names)
