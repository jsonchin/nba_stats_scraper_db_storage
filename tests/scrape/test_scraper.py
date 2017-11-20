import unittest

from db import config

config.DB_NAME = 'test_db'
config.DB_PATH = 'tests/db/databases'

import db.request_logger
import db.utils


class TestScraper(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        pass

    @classmethod
    def tearDownClass(cls):
        pass

    @classmethod
    def setUp(cls):
        pass

    def test_request_logger(self):
        db.request_logger.log_request('api_request', 'test_table')

    def test_already_scraped(self):
        api_request_query = db.utils.execute_sql("""SELECT api_request FROM scrape_log LIMIT 1;""").rows
        api_request = api_request_query[0][0]
        self.assertTrue(db.request_logger.already_scraped(api_request), 'Should have been scraped.')