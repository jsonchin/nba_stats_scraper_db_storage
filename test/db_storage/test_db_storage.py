import unittest
from db import config
config.DB_NAME = 'test_db'
config.DB_PATH = 'test/db_storage/databases'

from db import initialize
from db import store


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

    def test_db_init(self):
        """
        Tests if init_db creates a table called 'scrape_log'.
        """
        initialize.init_db()
        self.assertTrue(store.exists_table('scrape_log'), 'Table \'scrape_log\' should exist.')

    def test_exists_db(self):
        self.assertFalse(store.exists_table('TABLE_NAME_THAT_DOES_NOT_EXIST'),
                         'Table should not exist.')
