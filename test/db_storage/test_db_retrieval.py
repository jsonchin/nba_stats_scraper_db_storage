import unittest
from db_storage import db_config
db_config.DB_NAME = 'test_db'
db_config.DB_PATH = 'test/db_storage/databases'

from db_storage import db_init
from db_storage import db_storage
from db_storage import db_retrieval

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
        d = db_retrieval.fetch_player_ids()
        self.assertTrue(type(d) == dict or type(d) == defaultdict,
                        'Expected a dictionary got {}'.format(type(d)))

        SEASON_LENGTH = len('2017-18')
        self.assertTrue(all([len(key) == SEASON_LENGTH for key in d.keys()]))
