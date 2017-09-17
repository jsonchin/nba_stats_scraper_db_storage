import unittest
from db_storage import db_config
db_config.DB_NAME = 'test_db'
db_config.DB_PATH = 'test/db_storage/databases'

from scraper import request_logger

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

    def testRequestLogger(self):
        request_logger.log_request('api_request')
