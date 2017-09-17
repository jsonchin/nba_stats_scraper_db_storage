import unittest
from db import db_config
db_config.DB_NAME = 'test_db'
db_config.DB_PATH = 'test/db_storage/databases'

from scrape import request_logger
from scrape import scraper_init

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
        request_logger.log_request('api_request')

    @unittest.skip
    def test_scrape_player_ids(self):
        scraper_init.scrape_player_ids()
