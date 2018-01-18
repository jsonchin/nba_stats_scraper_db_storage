import unittest
from tests.test_setup import init_test_db

import db.initialize
import db.retrieve


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
