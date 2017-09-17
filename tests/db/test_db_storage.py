import unittest
from db import config
config.DB_NAME = 'test_db'
config.DB_PATH = 'tests/db/databases'

import db.initialize
import db.store


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
        db.initialize.init_db()
        self.assertTrue(db.store.exists_table('scrape_log'), 'Table \'scrape_log\' should exist.')

    def test_exists_db(self):
        self.assertFalse(db.store.exists_table('TABLE_NAME_THAT_DOES_NOT_EXIST'),
                         'Table should not exist.')
