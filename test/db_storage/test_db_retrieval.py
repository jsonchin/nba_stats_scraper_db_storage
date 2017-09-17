import unittest
from db import config
config.DB_NAME = 'test_db'
config.DB_PATH = 'test/db_storage/databases'

from db import initialize
from db import store
from db import retrieve

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
        d = retrieve.fetch_player_ids()
        self.assertTrue(type(d) == dict or type(d) == defaultdict,
                        'Expected a dictionary got {}'.format(type(d)))

        SEASON_LENGTH = len('2017-18')
        self.assertTrue(all([len(key) == SEASON_LENGTH for key in d.keys()]))
