import unittest
from db import config
config.DB_NAME = 'test_db'
config.DB_PATH = 'test/db_storage/databases'

import scrape.request_logger
import scrape.initialize

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
        scrape.request_logger.log_request('api_request')

    @unittest.skip
    def test_scrape_player_ids(self):
        scrape.initialize.scrape_player_ids()
